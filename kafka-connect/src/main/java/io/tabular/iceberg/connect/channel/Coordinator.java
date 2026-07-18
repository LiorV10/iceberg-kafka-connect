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

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  private final int totalPartitionCount;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;
  private final Collection<MemberDescription> members;
  private final Map<String, Integer> tableTopicPartitions = Maps.newHashMap();
  private final Map<TableIdentifier, Map<String, Set<Integer>>> pendingFlagVotes = Maps.newHashMap();
  private final Map<TableIdentifier, Map<String, Pair<TableContext, Map<String, Object>>>> pendingFlagData = Maps.newHashMap();

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

  /**
   * Commits all buffered work for this cycle.
   *
   * <p>The commit map holds, per table, the envelopes for each commit-id in first-seen
   * (chronological) order (see {@link CommitState#tableCommitMap()}). Historically this method
   * iterated tables as the outer loop and committed every commit-id for a table before moving on,
   * then committed the Kafka control-topic consumer offsets ONCE at the very end. That meant a
   * crash midway through a multi-commit-id cycle left the consumer offset unchanged, so
   * already-committed commit-ids were reprocessed (and re-committed) on restart.
   *
   * <p>This method now iterates <b>commit-id</b> as the outer loop, oldest-first. For each
   * commit-id it commits that commit-id's snapshot to <b>every</b> relevant table (in parallel),
   * and only once that commit-id is durably committed everywhere does it advance the control-topic
   * consumer offsets up to that commit-id's watermark (via {@link #commitConsumerOffsets(Map)}).
   * Because offsets are advanced strictly after a commit-id is fully committed, a crash between
   * commit-ids leaves the consumer positioned just past the last fully-committed commit-id, so
   * already-committed data is never re-committed. Advancing offsets outside the parallel per-table
   * loop also keeps the offset bookkeeping single-threaded and therefore trivially thread-safe.
   *
   * <p>Flags are still processed exactly once per table, AFTER all commit-ids' data has been
   * committed, since a flag broadcast's votes may be spread across multiple commit-ids.
   */
  private void doCommit(boolean partialCommit) {
    Map<TableIdentifier, Map<UUID, List<Envelope>>> commitMap = commitState.tableCommitMap();

    LOG.info("Commiting the following commits:");

    commitMap.forEach((t, m) -> m.forEach((id, events) -> {
      LOG.info("Commiting {} events at id {} for table {}", events.size(), id, t.toString());
    }));

    String offsetsJson = offsetsJson();
    OffsetDateTime vtts = commitState.vtts(partialCommit);

    // Pre-load tables and prepare per-table context (branch resolution, branch auto-create,
    // flag-vote denominator). Doing this once up-front lets the per-commit-id loop below stay
    // focused on committing data.
    Map<TableIdentifier, TableCommitContext> contexts = prepareTableContexts(commitMap);

    // Determine the global chronological order of commit-ids across all tables. Each table's inner
    // map preserves first-seen order; we union them while preserving that order so a commit-id that
    // was first seen earlier is committed (and has its offsets advanced) first.
    List<UUID> commitIdsInOrder = orderedCommitIds(commitMap);
    int lastIdx = commitIdsInOrder.size() - 1;

    for (int i = 0; i <= lastIdx; i++) {
      UUID commitId = commitIdsInOrder.get(i);
      boolean isLast = i == lastIdx;

      // Only the last (newest) commit-id records the offsets/vtts watermark as an Iceberg snapshot
      // property, matching the previous behavior; the consumer-offset watermark, by contrast, is
      // advanced after EVERY commit-id below.
      String commitOffsetsJson = isLast ? offsetsJson : null;
      OffsetDateTime commitVtts = isLast ? vtts : null;

      // Commit this commit-id's data to every table that has data for it, in parallel.
      List<TableCommitContext> tablesForCommitId =
          contexts.values().stream()
              .filter(ctx -> ctx.commitsById.containsKey(commitId))
              .collect(toList());

      Tasks.foreach(tablesForCommitId)
          .executeWith(exec)
          .stopOnFailure()
          .run(
              ctx ->
                  commitDataForCommitId(
                      ctx.table,
                      ctx.tableIdentifier,
                      ctx.branch,
                      commitId,
                      ctx.commitsById.get(commitId),
                      commitOffsetsJson,
                      commitVtts));

      // We only get here if this commit-id committed successfully to every relevant table. It is
      // now durable in Iceberg, so it is safe to advance the control-topic consumer offsets up to
      // this commit-id's watermark. A crash after this point will resume from here and will not
      // re-drive this (or any earlier) commit-id.
      Map<Integer, Long> commitIdOffsets = controlTopicOffsetsForCommitId(commitMap, commitId);
      commitConsumerOffsets(commitIdOffsets);
      LOG.info("Committed consumer offsets {} after commit-id {}", commitIdOffsets, commitId);
    }

    // Now that all data is committed, process any flags that became ready, exactly once per table.
    contexts.forEach(
        (tableIdentifier, ctx) -> {
          Map<String, Pair<TableContext, Map<String, Object>>> readyFlags =
              drainReadyFlags(tableIdentifier);
          if (!readyFlags.isEmpty()) {
            processFlagMessages(ctx.table, readyFlags);
            LOG.info(
                "Flags processed for table {} in commit {}, sending per-table resume signal",
                ctx.paramTableIdentifier, commitState.currentCommitId());
            Event flagSentinel =
                new Event(
                    config.controlGroupId(),
                    new CommitToTable(
                        FLAG_PROCESSED_SENTINEL_ID,
                        TableReference.of(config.catalogName(), ctx.paramTableIdentifier),
                        0L,
                        null));
            send(flagSentinel);
          }
        });

    // Final safety net: ensure the full high-watermark is committed so nothing is left behind (e.g.
    // control-topic offsets for records that carried no data for any commit-id, such as the
    // StartCommit/DataComplete control events themselves).
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

  /**
   * Resolves the global chronological order of commit-ids across all tables. Each table's inner map
   * preserves first-seen (chronological) order; unioning while preserving encounter order yields a
   * stable oldest-first ordering. Uses a {@link LinkedHashSet} so duplicates across tables collapse
   * to their first occurrence.
   */
  private List<UUID> orderedCommitIds(Map<TableIdentifier, Map<UUID, List<Envelope>>> commitMap) {
    Set<UUID> ordered = new LinkedHashSet<>();
    commitMap.values().forEach(byId -> ordered.addAll(byId.keySet()));
    return new ArrayList<>(ordered);
  }

  /**
   * Computes the control-topic consumer offset watermark (per control-topic partition) for a single
   * commit-id, as the max {@code envelope.offset() + 1} across every table's envelopes for that
   * commit-id. The {@code +1} matches the "next offset to consume" semantics used when populating
   * {@link #controlTopicOffsets()} in {@link #consumeAvailable}.
   */
  private Map<Integer, Long> controlTopicOffsetsForCommitId(
      Map<TableIdentifier, Map<UUID, List<Envelope>>> commitMap, UUID commitId) {
    Map<Integer, Long> offsets = Maps.newHashMap();
    commitMap
        .values()
        .forEach(
            byId -> {
              List<Envelope> envelopes = byId.get(commitId);
              if (envelopes != null) {
                envelopes.forEach(
                    envelope ->
                        offsets.merge(
                            envelope.partition(), envelope.offset() + 1, Math::max));
              }
            });
    return offsets;
  }

  private Map<TableIdentifier, TableCommitContext> prepareTableContexts(
      Map<TableIdentifier, Map<UUID, List<Envelope>>> commitMap) {
    Map<TableIdentifier, TableCommitContext> contexts = new LinkedHashMap<>();
    commitMap.forEach(
        (paramTableIdentifier, commitsById) -> {
          TableIdentifier tableIdentifier = paramTableIdentifier;
          Optional<String> branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

          if (this.config.dynamicBranchesEnabled()) {
            TableContext tableContext =
                TableContext.parse(tableIdentifier, this.config.branchesDelimiter());
            tableIdentifier = tableContext.tableIdentifier();
            branch = Optional.ofNullable(tableContext.branch());
          }

          Table table;
          try {
            table = catalog.loadTable(tableIdentifier);
          } catch (NoSuchTableException e) {
            LOG.warn("Table not found, skipping commit: {}", tableIdentifier);
            return;
          }

          if (branch.isPresent() && this.config.branchAutoCreateEnabled()) {
            try {
              table
                  .manageSnapshots()
                  .createBranch(branch.get(), table.history().get(0).snapshotId())
                  .commit();

              table
                  .newDelete()
                  .toBranch(branch.get())
                  .deleteFromRowFilter(Expressions.alwaysTrue())
                  .commit();

            } catch (IllegalArgumentException ignored) {
              // branch already exists
            }
          }

          // The full set of envelopes for this table across every commit-id. Used for the
          // flag-vote denominator (tableTopicPartitions) so it is independent of how envelopes
          // happen to be split across commit-ids.
          List<Envelope> allEnvelopesForTable =
              commitsById.values().stream().flatMap(List::stream).collect(toList());

          this.tableTopicPartitions.put(
              tableIdentifier.toString(),
              this.members.stream()
                  .mapToInt(
                      desc ->
                          (int)
                              desc.assignment().topicPartitions().stream()
                                  .filter(
                                      tp ->
                                          tp.topic()
                                              .equals(
                                                  Deduplicated.extractTopic(allEnvelopesForTable)))
                                  .count())
                  .sum());

          contexts.put(
              tableIdentifier,
              new TableCommitContext(
                  paramTableIdentifier, tableIdentifier, table, branch, commitsById));
        });
    return contexts;
  }

  /** Per-table state resolved once up-front and reused across the per-commit-id loop. */
  private static final class TableCommitContext {
    private final TableIdentifier paramTableIdentifier;
    private final TableIdentifier tableIdentifier;
    private final Table table;
    private final Optional<String> branch;
    private final Map<UUID, List<Envelope>> commitsById;

    private TableCommitContext(
        TableIdentifier paramTableIdentifier,
        TableIdentifier tableIdentifier,
        Table table,
        Optional<String> branch,
        Map<UUID, List<Envelope>> commitsById) {
      this.paramTableIdentifier = paramTableIdentifier;
      this.tableIdentifier = tableIdentifier;
      this.table = table;
      this.branch = branch;
      this.commitsById = commitsById;
    }
  }

  private String offsetsJson() {
    try {
      return MAPPER.writeValueAsString(controlTopicOffsets());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Commits the data and equality-delete files for a single commit-id to a single Iceberg snapshot.
   * Also accumulates (but does not drain) flag votes carried by this commit-id's envelopes. The
   * offsets/vtts watermark is written on this snapshot only when {@code offsetsJson}/{@code vtts}
   * are non-null, which the caller arranges for the last (newest) commit-id only.
   */
  private void commitDataForCommitId(
      Table table,
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

    // Accumulate flag votes for this commit-id; draining/processing happens once in doCommit
    // after all commit-ids' data has been committed.
    accumulateFlagVotes(commitId, tableIdentifier, filteredEnvelopeList);

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

  private void accumulateFlagVotes(UUID commitId, TableIdentifier tableIdentifier, List<Envelope> envelopes) {
    Map<String, Set<Integer>> partitionsThisCycle =
            Deduplicated.flagMessageSourcePartitions(envelopes, this.config.flagTypeField());
    LOG.debug("Accumulating {} flags", partitionsThisCycle.size());

    if (partitionsThisCycle.isEmpty()) {
      return;
    }

    Map<String, Pair<TableContext, Map<String, Object>>> dataThisCycle =
            Deduplicated.flagMessages(commitId, tableIdentifier,
                    envelopes, this.config.branchesDelimiter(), this.config.flagTypeField());

    Map<String, Set<Integer>> votes =
            pendingFlagVotes.computeIfAbsent(tableIdentifier, k -> Maps.newHashMap());
    Map<String, Pair<TableContext, Map<String, Object>>> data =
            pendingFlagData.computeIfAbsent(tableIdentifier, k -> Maps.newHashMap());

    partitionsThisCycle.forEach((type, newPartitions) -> {
      Set<Integer> accumulated =
              votes.computeIfAbsent(type, k -> new HashSet<>());
      accumulated.addAll(newPartitions);
      data.putIfAbsent(type, dataThisCycle.get(type));
      LOG.info("Flag '{}' for table {}: accumulated {}/{} unique partition votes (partitions: {})",
              type, tableIdentifier, accumulated.size(), tableTopicPartitions.getOrDefault(tableIdentifier.toString(), 3) , accumulated);
    });
  }

  private Map<String, Pair<TableContext, Map<String, Object>>> drainReadyFlags(
          TableIdentifier tableIdentifier) {
    Map<String, Set<Integer>> votes =
            pendingFlagVotes.getOrDefault(tableIdentifier, Maps.newHashMap());
    Map<String, Pair<TableContext, Map<String, Object>>> data =
            pendingFlagData.getOrDefault(tableIdentifier, Maps.newHashMap());

    List<String> readyTypes = votes.entrySet().stream()
            .filter(e -> e.getValue().size() >= tableTopicPartitions.getOrDefault(tableIdentifier.toString(), 3))
            .map(Map.Entry::getKey)
            .collect(toList());

    if (readyTypes.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Pair<TableContext, Map<String, Object>>> ready = Maps.newHashMap();
    readyTypes.forEach(type -> {
      ready.put(type, data.remove(type));
      votes.remove(type);
      LOG.info("Flag '{}' for table {} ready: all {} source partitions have reported it",
              type, tableIdentifier, tableTopicPartitions.getOrDefault(tableIdentifier.toString(), 3));
    });
    return ready;
  }

  private void processFlagMessages(Table table, Map<String, Pair<TableContext, Map<String, Object>>> flagMessages) {
    flagMessages.forEach((type, flagEntry) -> {
      TableContext flagMessage = flagEntry.first();
      Map<String, Object> flagEnvelope = flagEntry.second();
      @SuppressWarnings("unchecked")
      Map<String, Object> flagRecord = flagEnvelope.get("value") instanceof Map
              ? (Map<String, Object>) flagEnvelope.get("value")
              : flagEnvelope;
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
            }

            try {
              // Forward the branch: set current snapshot to the branch's snapshot
              // and clear the branch for further use
              table.manageSnapshots().setCurrentSnapshot(table.snapshot(targetBranch).snapshotId()).commit();
              table.manageSnapshots().removeBranch(targetBranch).commit();
              LOG.info("Successfully switched branch for table {} to {}", table.name(), targetBranch);
            } catch (Exception e) {
              LOG.error("Failed to switch branch for table {} to {}", table.name(), targetBranch, e);
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
      }
    });
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
