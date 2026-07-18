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
import io.tabular.iceberg.connect.FlagConfig;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import io.tabular.iceberg.connect.TableContext;
import io.tabular.iceberg.connect.data.SchemaUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends Channel implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String OFFSETS_SNAPSHOT_PROP_FMT = "kafka.connect.offsets.%s.%s";
  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VTTS_SNAPSHOT_PROP = "kafka.connect.vtts";
  private static final Duration POLL_DURATION = Duration.ofMillis(1000);
  static final UUID FLAG_PROCESSED_SENTINEL_ID = new UUID(0L, 0L);

  // Iceberg table property holding a JSON array of flag-identity markers that have already been
  // applied by processFlagMessages. Used to make flag application idempotent across a coordinator
  // crash/rebalance: if a coordinator dies after applying a flag but before offsets are durably
  // advanced, the next coordinator can see the marker and skip re-applying the (non-idempotent)
  // branch switch / schema mutation.
  private static final String APPLIED_FLAGS_PROP = "lakers.applied-flags";

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final int totalPartitionCount;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;
  private final Collection<MemberDescription> members;
  // Durable, rebalance-safe replacement for the former in-memory pendingFlagVotes / pendingFlagData
  // / tableTopicPartitions maps. Persists to Iceberg table properties so a coordinator elected
  // after a rebalance recovers in-progress flag votes instead of starting empty.
  private final FlagState flagState = new FlagState();

  public Coordinator(
          Catalog catalog,
          IcebergSinkConfig config,
          Collection<MemberDescription> members,
          KafkaClientFactory clientFactory,
          SinkTaskContext context) {
    // pass consumer group ID to which we commit low watermark offsets
    super("coordinator", config.controlGroupId() + "-coord", config, clientFactory, context);

    this.catalog = catalog;
    this.config = config;
    this.totalPartitionCount =
        members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
    this.snapshotOffsetsProp =
        String.format(OFFSETS_SNAPSHOT_PROP_FMT, config.controlTopic(), config.controlGroupId());
    this.exec = ThreadPools.newWorkerPool("iceberg-committer", config.commitThreads());
    this.commitState = new CommitState(config);
    this.members = members;

    // initial poll with longer duration so the consumer will initialize...
    consumeAvailable(Duration.ofMillis(1000), this::receive);
  }

  public void process() {
    if (commitState.isCommitIntervalReached()) {
      // send out begin commit
      commitState.startNewCommit();
      LOG.info("Started new commit with commit-id={}", commitState.currentCommitId().toString());
      Event event =
          new Event(config.controlGroupId(), new StartCommit(commitState.currentCommitId()));
      send(event);
      LOG.info("Sent workers commit trigger with commit-id={}", commitState.currentCommitId().toString());

    }

    consumeAvailable(POLL_DURATION, this::receive);

    if (commitState.isCommitTimedOut()) {
      commit(true);
    }
  }

  private boolean receive(Envelope envelope) {
    switch (envelope.event().type()) {
      case DATA_WRITTEN:
        commitState.addResponse(envelope);
        return true;
      case DATA_COMPLETE:
        commitState.addReady(envelope);
        if (commitState.isCommitReady(totalPartitionCount)) {
          commit(false);
        }
        return true;
    }
    return false;
  }

  private void commit(boolean partialCommit) {
    try {
      LOG.info("Processing commit after responses for {}, isPartialCommit {}",commitState.currentCommitId(), partialCommit);
      doCommit(partialCommit);
    } catch (Exception e) {
      LOG.warn("Commit failed, will try again next cycle", e);
    } finally {
      commitState.endCurrentCommit();
    }
  }

  private void doCommit(boolean partialCommit) {
    Map<TableIdentifier, Map<UUID, List<Envelope>>> commitMap = commitState.tableCommitMap();

    LOG.info("Commiting the following commits:");

    commitMap.forEach((t, m) -> m.forEach((id, events) -> {
      LOG.info("Commiting {} events at id {} for table {}", events.size(), id, t.toString());
    }));

    OffsetDateTime vtts = commitState.vtts(partialCommit);

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(entry -> commitToTable(entry.getKey(), entry.getValue(), partialCommit));

    // we should only get here if all tables committed successfully...
    commitConsumerOffsets();
    commitState.clearResponses();

    Event event =
        new Event(config.controlGroupId(), new CommitComplete(commitState.currentCommitId(), vtts));
    send(event);

    LOG.info(
        "Commit {} complete, committed to {} table(s), vtts {}",
        commitState.currentCommitId(),
        commitMap.size(),
        vtts);
  }

  private String offsetsJson(Map<Integer, Long> offsets) {
    try {
      return MAPPER.writeValueAsString(offsets);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * A small adapter exposing an Iceberg {@link Table}'s properties to {@link FlagState} through the
   * {@link FlagState.PropertyStore} interface. Writes are committed individually via
   * {@code updateProperties()} so the pending-flag document is durable the moment a vote is
   * accumulated, matching how offsets/watermarks are made durable elsewhere.
   */
  private static final class TablePropertyStore implements FlagState.PropertyStore {
    private final Table table;

    TablePropertyStore(Table table) {
      this.table = table;
    }

    @Override
    public String get(String key) {
      table.refresh();
      return table.properties().get(key);
    }

    @Override
    public void set(String key, String value) {
      table.updateProperties().set(key, value).commit();
    }

    @Override
    public void remove(String key) {
      table.updateProperties().remove(key).commit();
    }
  }

  /**
   * Commits all buffered work for a single table.
   *
   * <p>The {@code commitsById} map holds, per commit-id, the envelopes for that commit-id in
   * first-seen (chronological) order (see {@link CommitState#tableCommitMap()}). Each commit-id is
   * committed as its OWN Iceberg snapshot, oldest first, so that equality deletes from a later
   * commit land in a later snapshot (with a higher sequence number) than the data files they must
   * remove.
   *
   * <p>Every commit-id's snapshot records the offsets/vtts watermark, but the watermark is
   * ACCUMULATIVE up to (and including) that commit-id -- not the batch-wide watermark. Writing the
   * batch-wide watermark on an earlier snapshot would claim offsets whose data only lands in a later
   * snapshot; if the commit stops midway the recovered watermark would skip un-committed data. The
   * accumulative control-topic offsets are folded per control-partition as
   * {@code max(envelope.offset() + 1)} over commit-ids {@code 0..i}, and the accumulative vtts is
   * computed via {@link CommitState#vttsUpTo}. {@link #lastCommittedOffsetsForTable} determines
   * whether an envelope was already committed, so recording the watermark per commit-id is safe.
   *
   * <p>Flags, by contrast, must be processed exactly ONCE per table, AFTER all data snapshots for
   * this batch have been committed:
   *
   * <ul>
   *   <li>A single flag broadcast (e.g. END-LOAD / DDL) arrives as one DataWritten per source
   *       partition, and those partitions may be spread across multiple commit-ids. So flag votes
   *       are accumulated across ALL commit-ids first, and only then drained.
   *   <li>Flag processing (branch switch via setCurrentSnapshot/removeBranch, schema updates) must
   *       run after the data it relates to is committed; draining flags inside the per-commit-id
   *       loop could switch the branch before later commit-ids' data is appended.
   * </ul>
   *
   * <p>Flag vote/payload state and the vote denominator are accumulated through {@link #flagState},
   * which persists them to this table's Iceberg properties. This means a coordinator elected after
   * a rebalance recovers any partial votes for the table instead of starting empty, so multi-cycle
   * flag broadcasts are never lost mid-flight.
   */
  private void commitToTable(
      TableIdentifier paramTableIdentifier,
      Map<UUID, List<Envelope>> commitsById,
      boolean partialCommit) {
    Table table;
    TableIdentifier tableIdentifier = paramTableIdentifier;
    Optional<String> branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

    if (this.config.dynamicBranchesEnabled()) {
      TableContext tableContext = TableContext.parse(tableIdentifier, this.config.branchesDelimiter());
      tableIdentifier = tableContext.tableIdentifier();
      branch = Optional.ofNullable(tableContext.branch());
    }

    try {
      table = catalog.loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      LOG.warn("Table not found, skipping commit: {}", tableIdentifier);
      return;
    }

    if (branch.isPresent() && this.config.branchAutoCreateEnabled()) {
      try {
        table.manageSnapshots().createBranch(branch.get(), table.history().get(0).snapshotId()).commit();

        table.newDelete()
                .toBranch(branch.get())
                .deleteFromRowFilter(Expressions.alwaysTrue())
                .commit();

      } catch (IllegalArgumentException ignored) {
        // branch already exists
      }
    }

    // Durable store backed by this table's Iceberg properties, used for both flag-vote persistence
    // (via FlagState) and the applied-flag idempotency markers.
    FlagState.PropertyStore flagStore = new TablePropertyStore(table);

    // The full set of envelopes for this table across every commit-id. Used for the flag-vote
    // denominator (tableTopicPartitions) so it is independent of how envelopes happen to be split
    // across commit-ids.
    List<Envelope> allEnvelopesForTable =
        commitsById.values().stream().flatMap(List::stream).collect(toList());

    int sourcePartitionCount =
        this.members.stream()
            .mapToInt(
                desc ->
                    (int)
                        desc.assignment().topicPartitions().stream()
                            .filter(
                                tp ->
                                    tp.topic()
                                        .equals(Deduplicated.extractTopic(allEnvelopesForTable)))
                            .count())
            .sum();
    // Persist the vote denominator so a coordinator elected after a rebalance uses the same
    // threshold the in-flight votes were being counted against (never a hard-coded fallback).
    if (sourcePartitionCount > 0) {
      flagState.setTableTopicPartitions(flagStore, tableIdentifier, sourcePartitionCount);
    }

    // Commit each commit-id as its own snapshot, oldest first, accumulating flag votes as we go but
    // NOT draining them until all data snapshots for this table are committed. Each commit-id's
    // snapshot records an ACCUMULATIVE offsets/vtts watermark up to (and including) that commit-id.
List<UUID> commitIdsInOrder = new ArrayList<>(commitsById.keySet());
// Running control-topic offset watermark, folded across commit-ids as we iterate oldest first.
Map<Integer, Long> accumulatedOffsets =
    Maps.newHashMap(lastCommittedOffsetsForTable(table, branch.orElse(null)));
    int lastIdx = commitIdsInOrder.size() - 1;
    for (int i = 0; i <= lastIdx; i++) {
      UUID commitId = commitIdsInOrder.get(i);
      List<Envelope> envelopes = commitsById.get(commitId);

      // Fold this commit-id's control-topic offsets into the running watermark. The consumer stores
      // the offset of the NEXT record to consume, so use offset + 1 (see Channel.consumeAvailable).
      for (Envelope envelope : envelopes) {
        accumulatedOffsets.merge(envelope.partition(), envelope.offset() + 1, Math::max);
      }

      // Accumulative vtts up to and including this commit-id.
      OffsetDateTime accumulatedVtts =
          commitState.vttsUpTo(commitIdsInOrder.subList(0, i + 1), partialCommit);

      commitDataForCommitId(
          table,
          flagStore,
          tableIdentifier,
          branch,
          commitId,
          envelopes,
          offsetsJson(accumulatedOffsets),
          accumulatedVtts);
    }

    // Now that all data is committed, process any flags that became ready, exactly once.
    Map<String, Pair<TableContext, Map<String, Object>>> readyFlags =
        flagState.drainReady(flagStore, tableIdentifier);
    if (!readyFlags.isEmpty()) {
      processFlagMessages(table, flagStore, readyFlags);
      LOG.info(
              "Flags processed for table {} in commit {}, sending per-table resume signal",
              paramTableIdentifier, commitState.currentCommitId());
      Event flagSentinel =
              new Event(
                      config.controlGroupId(),
                      new CommitToTable(
                              FLAG_PROCESSED_SENTINEL_ID,
                              TableReference.of(config.catalogName(), paramTableIdentifier),
                              0L,
                              null));
      send(flagSentinel);
    }
  }

  /**
   * Commits the data and equality-delete files for a single commit-id to a single Iceberg snapshot.
   * Also accumulates (but does not drain) flag votes carried by this commit-id's envelopes. The
   * offsets/vtts watermark is written on this snapshot whenever {@code offsetsJson}/{@code vtts}
   * are non-null; the caller passes the ACCUMULATIVE watermark up to this commit-id. Since
   * {@link #lastCommittedOffsetsForTable} determines whether an envelope was already committed,
   * persisting the accumulative watermark on every commit-id's snapshot is safe.
   */
  private void commitDataForCommitId(
      Table table,
      FlagState.PropertyStore flagStore,
      TableIdentifier tableIdentifier,
      Optional<String> branch,
      UUID commitId,
      List<Envelope> envelopeList,
      String offsetsJson,
      OffsetDateTime vtts) {
    Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(table, branch.orElse(null));

    List<Envelope> filteredEnvelopeList =
        envelopeList.stream()
            .filter(
                envelope -> {
                  Long minOffset = committedOffsets.get(envelope.partition());
                  return minOffset == null || envelope.offset() >= minOffset;
                })
            .collect(toList());

    List<DataFile> dataFiles =
        Deduplicated.dataFiles(commitId, tableIdentifier, filteredEnvelopeList)
            .stream()
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());

    List<DeleteFile> deleteFiles =
        Deduplicated.deleteFiles(
                commitId, tableIdentifier, filteredEnvelopeList)
            .stream()
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .collect(toList());

    // Accumulate flag votes for this commit-id; draining/processing happens once in commitToTable
    // after all commit-ids' data has been committed. Persisted via flagStore so votes survive a
    // rebalance.
    accumulateFlagVotes(flagStore, commitId, tableIdentifier, filteredEnvelopeList);

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Nothing to commit to table {} for commit-id {}, skipping", tableIdentifier, commitId);
      return;
    }

    if (deleteFiles.isEmpty()) {
      Transaction transaction = table.newTransaction();

      Map<Integer, List<DataFile>> filesBySpec =
          dataFiles.stream()
              .collect(Collectors.groupingBy(DataFile::specId, Collectors.toList()));

      List<List<DataFile>> list = Lists.newArrayList(filesBySpec.values());
      int lastIdx = list.size() - 1;
      for (int i = 0; i <= lastIdx; i++) {
        AppendFiles appendOp = transaction.newAppend();
        branch.ifPresent(appendOp::toBranch);

        list.get(i).forEach(appendOp::appendFile);
        appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitId.toString());
        if (i == lastIdx) {
          if (offsetsJson != null) {
            appendOp.set(snapshotOffsetsProp, offsetsJson);
          }
          if (vtts != null) {
            appendOp.set(VTTS_SNAPSHOT_PROP, Long.toString(vtts.toInstant().toEpochMilli()));
          }
        }

        appendOp.commit();
      }

      transaction.commitTransaction();
    } else {
      RowDelta deltaOp = table.newRowDelta();
      branch.ifPresent(deltaOp::toBranch);
      if (offsetsJson != null) {
        deltaOp.set(snapshotOffsetsProp, offsetsJson);
      }
      deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitId.toString());
      if (vtts != null) {
        deltaOp.set(VTTS_SNAPSHOT_PROP, Long.toString(vtts.toInstant().toEpochMilli()));
      }
      dataFiles.forEach(deltaOp::addRows);
      deleteFiles.forEach(deltaOp::addDeletes);
      deltaOp.commit();
    }

    Long snapshotId = latestSnapshot(table, branch.orElse(null)).snapshotId();
    Event event =
        new Event(
            config.controlGroupId(),
            new CommitToTable(
                commitId,
                TableReference.of(config.catalogName(), tableIdentifier),
                snapshotId,
                vtts));
    send(event);

    LOG.info(
        "Commit complete to table {}, snapshot {}, commit ID {}, vtts {}",
        tableIdentifier,
        snapshotId,
        commitId,
        vtts);
  }

  private void accumulateFlagVotes(
      FlagState.PropertyStore flagStore,
      UUID commitId,
      TableIdentifier tableIdentifier,
      List<Envelope> envelopes) {
    Map<String, Set<Integer>> partitionsThisCycle =
            Deduplicated.flagMessageSourcePartitions(envelopes, this.config.flagTypeField());
    LOG.debug("Accumulating {} flags", partitionsThisCycle.size());

    if (partitionsThisCycle.isEmpty()) {
      return;
    }

    Map<String, Pair<TableContext, Map<String, Object>>> dataThisCycle =
            Deduplicated.flagMessages(commitId, tableIdentifier,
                    envelopes, this.config.branchesDelimiter(), this.config.flagTypeField());

    flagState.accumulate(flagStore, tableIdentifier, partitionsThisCycle, dataThisCycle);

    partitionsThisCycle.forEach((type, newPartitions) ->
        LOG.info("Flag '{}' for table {}: accumulated partition votes {} this cycle",
            type, tableIdentifier, newPartitions));
  }

  /**
   * Applies the given ready flags to the table. Each side effect is guarded so that reprocessing an
   * already-applied flag after a crash/rebalance is a safe no-op:
   *
   * <ul>
   *   <li>a durable per-flag marker in {@link #APPLIED_FLAGS_PROP} short-circuits an entire re-apply;
   *   <li>the branch switch only runs if the target branch still exists;
   *   <li>the pending column rewrite only runs if the {@code _pending_type_update} columns are still
   *       present.
   * </ul>
   *
   * Because applying a flag and durably advancing Kafka offsets are not atomic, this targets
   * at-least-once delivery with an idempotency guard rather than exactly-once.
   */
  private void processFlagMessages(
      Table table,
      FlagState.PropertyStore flagStore,
      Map<String, Pair<TableContext, Map<String, Object>>> flagMessages) {
    flagMessages.forEach((type, flagEntry) -> {
      TableContext flagMessage = flagEntry.first();
      Map<String, Object> flagEnvelope = flagEntry.second();
      @SuppressWarnings("unchecked")
      Map<String, Object> flagRecord = flagEnvelope.get("value") instanceof Map
              ? (Map<String, Object>) flagEnvelope.get("value")
              : flagEnvelope;

      String appliedMarker = flagAppliedMarker(type, flagMessage, flagEnvelope);
      if (isFlagAlreadyApplied(flagStore, appliedMarker)) {
        LOG.info(
            "Flag of type {} for table {} already applied (marker {}), skipping",
            type, flagMessage.tableIdentifier(), appliedMarker);
        return;
      }

      LOG.debug("About to process flag of type {} for: {}", type, flagMessage.tableIdentifier().toString());

      switch (type) {
        case "END-LOAD":
          String targetBranch = flagMessage.branch();
          if (targetBranch != null) {
            LOG.info("Processing flag message for table {}, switching to branch {}",
                    table.name(), targetBranch);


            List<Types.NestedField> pending = table.schema().columns()
                    .stream()
                    .filter(field -> field.name().endsWith("_pending_type_update"))
                    .collect(toList());

            if (!pending.isEmpty()) {
              UpdateSchema updateSchemaCommit = table.updateSchema();

              pending.forEach(field -> {
                String original = field.name().split("_pending_type_update")[0];

                updateSchemaCommit.deleteColumn(original).renameColumn(field.name(), original);
              });

              try {
                updateSchemaCommit.commit();
                LOG.info("Successfully updated types for table {}", table.name());
              } catch (Exception e) {
                LOG.error("Failed to update types for table {}. {}", table.name(), e.getMessage());
              }
            } else {
              LOG.info(
                  "No pending type updates for table {}, skipping schema rewrite (already applied?)",
                  table.name());
            }

            // Only switch the branch if it still exists; a previous (crashed) coordinator may have
            // already removed it, in which case the switch was already applied.
            if (table.snapshot(targetBranch) != null) {
              try {
                // Forward the branch: set current snapshot to the branch's snapshot
                // and clear the branch for further use
                table.manageSnapshots().setCurrentSnapshot(table.snapshot(targetBranch).snapshotId()).commit();
                table.manageSnapshots().removeBranch(targetBranch).commit();
                LOG.info("Successfully switched branch for table {} to {}", table.name(), targetBranch);
              } catch (Exception e) {
                LOG.error("Failed to switch branch for table {} to {}", table.name(), targetBranch, e);
              }
            } else {
              LOG.info(
                  "Branch {} no longer exists for table {}, skipping branch switch (already applied?)",
                  targetBranch, table.name());
            }
          }
          break;
        case "DDL":
          FlagConfig flagConfig = this.config.flagConfig();

          List<Map<String, Object>> fields = (List<Map<String, Object>>) flagRecord.get(flagConfig.getFields());
          List<String> pks = fields.stream()
                  .filter(field -> field.get(flagConfig.getKeyFlag()).equals("X"))
                  .map(field -> field.get(flagConfig.getFieldName()).toString().toLowerCase())
                  .collect(toList());

          table.updateProperties().set("lakers.id-cols", String.join(",", pks)).commit();

          List<Map<String, Object>> fields_modified = (List<Map<String, Object>>) flagRecord.get(flagConfig.getFieldsModified());

          if (fields_modified != null && !fields_modified.isEmpty()) {
            UpdateSchema updateSchemaCommit = table.updateSchema();
            fields_modified.forEach(field -> {
              LOG.debug("{} Modified, value {}, inferred type: {}",
                  field.get(flagConfig.getFieldName()).toString(),
                      field.get(flagConfig.getTypeValue()),
                  SchemaUtils.inferIcebergType(field.get(flagConfig.getTypeValue()), this.config).orElse(Types.BinaryType.get())
              );
              updateSchemaCommit.addColumn(
                      field.get(flagConfig.getFieldName()).toString() + "_pending_type_update",
                      SchemaUtils.inferIcebergType(field.get(flagConfig.getTypeValue()), this.config)
                              .orElse(Types.StringType.get())
              );
            });

            updateSchemaCommit.commit();
          }

          table.updateProperties().set("lakers.last-schema-change", ZonedDateTime.now(ZoneOffset.UTC)
                  .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))).commit();

          break;
        default:
          LOG.error("Couldn't process flag of type {}", type);
          return;
      }

      // Record the durable applied-marker only after the mutations above have been committed, so a
      // crash mid-apply leaves the marker absent and the guarded, idempotent re-apply can complete.
      markFlagApplied(flagStore, appliedMarker);
    });
  }

  /**
   * Builds a stable identity string for an applied flag, derived from the flag type, table/branch,
   * and the source record's topic/partition/offset (when available). Two invocations for the same
   * logical flag broadcast produce the same marker, so the applied-marker check is deterministic
   * across coordinator instances.
   */
  private String flagAppliedMarker(
      String type, TableContext flagMessage, Map<String, Object> flagEnvelope) {
    Object topic = flagEnvelope.get("topic");
    Object partition = flagEnvelope.get("partition");
    Object offset = flagEnvelope.get("offset");
    return String.join(
        "|",
        type,
        String.valueOf(flagMessage.tableIdentifier()),
        String.valueOf(flagMessage.branch()),
        String.valueOf(topic),
        String.valueOf(partition),
        String.valueOf(offset));
  }

  private boolean isFlagAlreadyApplied(FlagState.PropertyStore flagStore, String marker) {
    return readAppliedFlags(flagStore).contains(marker);
  }

  private void markFlagApplied(FlagState.PropertyStore flagStore, String marker) {
    Set<String> applied = new LinkedHashSet<>(readAppliedFlags(flagStore));
    if (applied.add(marker)) {
      try {
        flagStore.set(APPLIED_FLAGS_PROP, MAPPER.writeValueAsString(new ArrayList<>(applied)));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private List<String> readAppliedFlags(FlagState.PropertyStore flagStore) {
    String json = flagStore.get(APPLIED_FLAGS_PROP);
    if (json == null || json.isEmpty()) {
      return Collections.emptyList();
    }
    try {
      return MAPPER.readValue(json, new TypeReference<List<String>>() {});
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Snapshot latestSnapshot(Table table, String branch) {
    if (branch == null) {
      return table.currentSnapshot();
    }
    return table.snapshot(branch);
  }

  private Map<Integer, Long> lastCommittedOffsetsForTable(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);
    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String value = summary.get(snapshotOffsetsProp);
      if (value != null) {
        TypeReference<Map<Integer, Long>> typeRef = new TypeReference<Map<Integer, Long>>() {};
        try {
          return MAPPER.readValue(value, typeRef);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return ImmutableMap.of();
  }

  @Override
  public void close() throws IOException {
    exec.shutdownNow();
    stop();
  }
}
