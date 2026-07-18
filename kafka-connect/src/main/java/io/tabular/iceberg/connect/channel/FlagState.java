/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.channel;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.TableContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the coordinator's cross-cycle flag-accumulation state and makes it durable.
 *
 * <p>A flag broadcast (e.g. {@code END-LOAD} / {@code DDL}) arrives as one {@code DataWritten} per
 * source partition, spread across potentially many commit cycles. A flag is only "ready" once every
 * source partition for a table has voted for it. Historically this vote state lived only in
 * in-memory maps on {@link Coordinator}, so any Kafka Connect rebalance that moved the coordinator
 * role to another task discarded partially-accumulated votes -- the flag could then never reach
 * quorum, and its branch switch / schema update would never run.
 *
 * <p>{@code FlagState} keeps the same three maps but persists them to <strong>Iceberg table
 * properties</strong> (a JSON blob under {@link #PENDING_FLAGS_PROP}), mirroring how the connector
 * already stamps {@code lakers.id-cols} and {@code lakers.last-schema-change}. A newly-elected
 * coordinator reloads this state from the table (see {@link #loadForTable}) and resumes
 * accumulation exactly where the previous coordinator left off.
 *
 * <p>Persistence granularity is per table: each table stores its own pending-flag document, loaded
 * lazily the first time the table is touched in this coordinator instance and rewritten after every
 * mutation. When a flag is drained/applied its entry is removed and the document rewritten (or the
 * property removed entirely when no pending flags remain), so an applied flag is never re-counted.
 *
 * <p>This class deliberately mirrors the shape and conventions of {@link CommitState}: it is a
 * plain state holder with no Kafka or catalog dependencies of its own. The persistence read/write
 * is performed through the small {@link PropertyStore} abstraction so it can be unit-tested against
 * an in-memory map and driven against a real Iceberg {@code Table} by the coordinator.
 */
public class FlagState {

  private static final Logger LOG = LoggerFactory.getLogger(FlagState.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Iceberg table property under which the pending-flag document for a table is stored. */
  public static final String PENDING_FLAGS_PROP = "lakers.pending-flags";

  /**
   * Minimal read/write view over a durable key/value store (in practice, Iceberg table
   * properties). Kept tiny so {@link FlagState} stays free of catalog dependencies and is trivially
   * unit-testable.
   */
  public interface PropertyStore {
    /** Returns the current value for {@code key}, or {@code null} if unset. */
    String get(String key);

    /** Durably sets {@code key} to {@code value}. */
    void set(String key, String value);

    /** Durably removes {@code key} if present. */
    void remove(String key);
  }

  // Per-table set of source partitions that have voted for each flag type.
  private final Map<TableIdentifier, Map<String, Set<Integer>>> pendingFlagVotes =
      Maps.newHashMap();
  // Per-table payload (context + record) for each flag type, kept until the flag is applied.
  private final Map<TableIdentifier, Map<String, Pair<TableContext, Map<String, Object>>>>
      pendingFlagData = Maps.newHashMap();
  // Per-table source-partition count used as the vote denominator (quorum threshold).
  private final Map<String, Integer> tableTopicPartitions = Maps.newHashMap();

  // Tables whose persisted state has already been read into memory this coordinator instance, so we
  // load each table's document from the store at most once.
  private final Set<TableIdentifier> loaded = new HashSet<>();

  /**
   * Records the source-partition count (vote denominator) for a table. Persisted so a coordinator
   * elected after a rebalance uses the same threshold that the in-flight votes were being counted
   * against, rather than recomputing it from whatever happens to be in the current batch.
   */
  public void setTableTopicPartitions(
      PropertyStore store, TableIdentifier tableIdentifier, int partitionCount) {
    loadForTable(store, tableIdentifier);
    tableTopicPartitions.put(tableIdentifier.toString(), partitionCount);
    persist(store, tableIdentifier);
  }

  /**
   * Accumulates this cycle's votes/payloads for a table and persists the merged state.
   *
   * @param newVotes flag type -> newly-voting source partitions observed this cycle
   * @param newData flag type -> payload for that flag (only recorded the first time seen)
   */
  public void accumulate(
      PropertyStore store,
      TableIdentifier tableIdentifier,
      Map<String, Set<Integer>> newVotes,
      Map<String, Pair<TableContext, Map<String, Object>>> newData) {
    if (newVotes.isEmpty()) {
      return;
    }
    loadForTable(store, tableIdentifier);

    Map<String, Set<Integer>> votes =
        pendingFlagVotes.computeIfAbsent(tableIdentifier, k -> Maps.newHashMap());
    Map<String, Pair<TableContext, Map<String, Object>>> data =
        pendingFlagData.computeIfAbsent(tableIdentifier, k -> Maps.newHashMap());

    newVotes.forEach(
        (type, partitions) -> {
          Set<Integer> accumulated = votes.computeIfAbsent(type, k -> new HashSet<>());
          accumulated.addAll(partitions);
          if (newData.containsKey(type)) {
            data.putIfAbsent(type, newData.get(type));
          }
        });

    persist(store, tableIdentifier);
  }

  /**
   * Removes and returns the flag types for a table that have reached quorum (all source partitions
   * have voted), persisting the reduced state. Because a drained flag's votes and payload are
   * removed and the document rewritten, re-invoking after a crash/rebalance will not re-drain an
   * already-applied flag (its entry is gone from the store).
   */
  public Map<String, Pair<TableContext, Map<String, Object>>> drainReady(
      PropertyStore store, TableIdentifier tableIdentifier) {
    loadForTable(store, tableIdentifier);

    Map<String, Set<Integer>> votes =
        pendingFlagVotes.getOrDefault(tableIdentifier, Collections.emptyMap());
    Map<String, Pair<TableContext, Map<String, Object>>> data =
        pendingFlagData.getOrDefault(tableIdentifier, Collections.emptyMap());

    Integer threshold = tableTopicPartitions.get(tableIdentifier.toString());

    if (threshold == null) {
      return Collections.emptyMap();
    }

    List<String> readyTypes =
        votes.entrySet().stream()
            .filter(e -> e.getValue().size() >= threshold)
            .map(Map.Entry::getKey)
            .collect(toList());

    if (readyTypes.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Pair<TableContext, Map<String, Object>>> ready = Maps.newHashMap();
    readyTypes.forEach(
        type -> {
          ready.put(type, data.remove(type));
          votes.remove(type);
          LOG.info(
              "Flag '{}' for table {} ready: all {} source partitions have reported it",
              type,
              tableIdentifier,
              threshold);
        });

    persist(store, tableIdentifier);
    return ready;
  }

  /** Test/inspection helper: current accumulated votes for a table (empty if none). */
  Map<String, Set<Integer>> votesForTable(TableIdentifier tableIdentifier) {
    return pendingFlagVotes.getOrDefault(tableIdentifier, Collections.emptyMap());
  }

  /**
   * Loads a table's persisted pending-flag document into memory, once per table per coordinator
   * instance. Subsequent calls are no-ops. This is what lets a coordinator elected after a
   * rebalance transparently continue accumulation.
   */
  void loadForTable(PropertyStore store, TableIdentifier tableIdentifier) {
    if (!loaded.add(tableIdentifier)) {
      return;
    }
    String json = store.get(PENDING_FLAGS_PROP);
    if (json == null || json.isEmpty()) {
      return;
    }
    try {
      PersistedTableFlags persisted =
          MAPPER.readValue(json, new TypeReference<PersistedTableFlags>() {});
      if (persisted == null) {
        return;
      }
      if (persisted.partitionCount != null) {
        tableTopicPartitions.put(tableIdentifier.toString(), persisted.partitionCount);
      }
      if (persisted.votes != null && !persisted.votes.isEmpty()) {
        Map<String, Set<Integer>> votes = Maps.newHashMap();
        persisted.votes.forEach((type, parts) -> votes.put(type, new HashSet<>(parts)));
        pendingFlagVotes.put(tableIdentifier, votes);
      }
      if (persisted.data != null && !persisted.data.isEmpty()) {
        Map<String, Pair<TableContext, Map<String, Object>>> data = Maps.newHashMap();
        persisted.data.forEach(
            (type, entry) ->
                data.put(
                    type,
                    Pair.of(
                        new TableContext(
                            TableIdentifier.parse(entry.tableIdentifier), entry.branch),
                        entry.record)));
        pendingFlagData.put(tableIdentifier, data);
      }
      LOG.info(
          "Recovered pending flag state for table {}: {} flag type(s) with partial votes",
          tableIdentifier,
          persisted.votes == null ? 0 : persisted.votes.size());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Serializes and durably writes a table's current pending-flag state, or removes it if empty. */
  private void persist(PropertyStore store, TableIdentifier tableIdentifier) {
    Map<String, Set<Integer>> votes =
        pendingFlagVotes.getOrDefault(tableIdentifier, Collections.emptyMap());
    Map<String, Pair<TableContext, Map<String, Object>>> data =
        pendingFlagData.getOrDefault(tableIdentifier, Collections.emptyMap());
    Integer partitionCount = tableTopicPartitions.get(tableIdentifier.toString());

    if (votes.isEmpty() && data.isEmpty() && partitionCount == null) {
      store.remove(PENDING_FLAGS_PROP);
      return;
    }

    PersistedTableFlags persisted = new PersistedTableFlags();
    persisted.partitionCount = partitionCount;
    persisted.votes = new LinkedHashMap<>();
    votes.forEach((type, parts) -> persisted.votes.put(type, new HashSet<>(parts)));
    persisted.data = new LinkedHashMap<>();
    data.forEach(
        (type, pair) -> {
          PersistedFlagData entry = new PersistedFlagData();
          entry.tableIdentifier = pair.first().tableIdentifier().toString();
          entry.branch = pair.first().branch();
          entry.record = pair.second();
          persisted.data.put(type, entry);
        });

    try {
      store.set(PENDING_FLAGS_PROP, MAPPER.writeValueAsString(persisted));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** JSON shape persisted per table under {@link #PENDING_FLAGS_PROP}. */
  static class PersistedTableFlags {
    public Integer partitionCount;
    public Map<String, Set<Integer>> votes;
    public Map<String, PersistedFlagData> data;
  }

  /** JSON shape for a single flag type's payload. */
  static class PersistedFlagData {
    public String tableIdentifier;
    public String branch;
    public Map<String, Object> record;
  }
}
