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
import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import io.tabular.iceberg.connect.TableContext;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.kafka.clients.admin.MemberDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Coordinator extends Channel implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String OFFSETS_SNAPSHOT_PROP_FMT = "kafka.connect.offsets.%s.%s";

  private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
  private static final String VTTS_SNAPSHOT_PROP = "kafka.connect.vtts";
  private static final Duration POLL_DURATION = Duration.ofMillis(1000);

  private final Catalog catalog;
  private final IcebergSinkConfig config;
  // Total number of source-topic partitions across all tasks in this consumer group.
  // There is exactly ONE coordinator per sink connector (the task that owns the lowest
  // TopicPartition elected as leader). All other tasks are workers that write data files
  // and report them via the control topic. The coordinator receives DataWritten events
  // from EVERY task and waits for isCommitReady(totalPartitionCount) before committing.
  //
  // Flag ordering guarantee:
  //   The producer must broadcast the flag to ALL source partitions (one copy per partition).
  //   Each partition's flag becomes one FlagWriterResult in the task that processes it, and
  //   therefore exactly ONE DataWritten event on the control topic.  Across all tasks the
  //   coordinator accumulates these DataWritten-flag events until their count reaches
  //   totalPartitionCount (one per source partition), at which point every task has seen and
  //   activated its reroute, and all pre-flag data has been committed — only then is the flag
  //   action (e.g. branch switch) executed.
  private final int totalPartitionCount;
  private final String snapshotOffsetsProp;
  private final ExecutorService exec;
  private final CommitState commitState;

  // Accumulated vote counts per table per flag type across commit cycles.
  // Keyed by table identifier → (flag type → number of flag DataWritten events seen so far).
  // One DataWritten event = one source partition's flag record (broadcast copy).
  private final Map<TableIdentifier, Map<String, Integer>> pendingFlagVotes = Maps.newHashMap();
  // First-seen flag payload per table per flag type, held until the vote count is complete.
  private final Map<TableIdentifier, Map<String, Pair<TableContext, Map<String, Object>>>> pendingFlagData = Maps.newHashMap();

  public Coordinator(
      Catalog catalog,
      IcebergSinkConfig config,
      Collection<MemberDescription> members,
      KafkaClientFactory clientFactory) {
    // pass consumer group ID to which we commit low watermark offsets
    super("coordinator", config.controlGroupId() + "-coord", config, clientFactory);

    this.catalog = catalog;
    this.config = config;
    this.totalPartitionCount =
        members.stream().mapToInt(desc -> desc.assignment().topicPartitions().size()).sum();
    this.snapshotOffsetsProp =
        String.format(OFFSETS_SNAPSHOT_PROP_FMT, config.controlTopic(), config.controlGroupId());
    this.exec = ThreadPools.newWorkerPool("iceberg-committer", config.commitThreads());
    this.commitState = new CommitState(config);

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
    Map<TableIdentifier, List<Envelope>> commitMap = commitState.tableCommitMap();

    String offsetsJson = offsetsJson();
    OffsetDateTime vtts = commitState.vtts(partialCommit);

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(entry -> commitToTable(entry.getKey(), entry.getValue(), offsetsJson, vtts));

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

  private String offsetsJson() {
    try {
      return MAPPER.writeValueAsString(controlTopicOffsets());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Commits data and flag messages for a single table.
   */
  private void commitToTable(
      TableIdentifier paramTableIdentifier,
      List<Envelope> envelopeList,
      String offsetsJson,
      OffsetDateTime vtts) {
    Table table;
    TableIdentifier tableIdentifier = paramTableIdentifier;
    Optional<String> branch = config.tableConfig(tableIdentifier.toString()).commitBranch();

    if (this.config.dynamicBranchesEnabled()) {
      TableContext tableContext = TableContext.parse(tableIdentifier, this.config.branchesRegexDelimiter());
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
      } catch (IllegalArgumentException ignored) {
        // branch already exists
      }
    }

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
        Deduplicated.dataFiles(commitState.currentCommitId(), tableIdentifier, filteredEnvelopeList)
            .stream()
            .filter(dataFile -> dataFile.recordCount() > 0)
            .collect(toList());

    List<DeleteFile> deleteFiles =
        Deduplicated.deleteFiles(
                commitState.currentCommitId(), tableIdentifier, filteredEnvelopeList)
            .stream()
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .collect(toList());

    // Accumulate flag votes from this cycle. Each source partition's broadcast flag copy
    // produces one DataWritten event (= one vote). A flag is only processed once the
    // accumulated vote count reaches totalPartitionCount (all partitions have reported).
    // Because different tasks receive the broadcast flag in different commit cycles, votes
    // are persisted across cycles until the quorum is reached.
    accumulateFlagVotes(tableIdentifier, filteredEnvelopeList);

    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      LOG.info("Nothing to commit to table {}, skipping", tableIdentifier);
    } else {
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
          appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
          if (i == lastIdx) {
            appendOp.set(snapshotOffsetsProp, offsetsJson);
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
        deltaOp.set(snapshotOffsetsProp, offsetsJson);
        deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, commitState.currentCommitId().toString());
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
                  commitState.currentCommitId(),
                  TableReference.of(config.catalogName(), tableIdentifier),
                  snapshotId,
                  vtts));
      send(event);

      LOG.info(
          "Commit complete to table {}, snapshot {}, commit ID {}, vtts {}",
          tableIdentifier,
          snapshotId,
          commitState.currentCommitId(),
          vtts);
    }

    // Process flag messages for which all tasks have now voted.
    Map<String, Pair<TableContext, Map<String, Object>>> readyFlags = drainReadyFlags(tableIdentifier);
    if (!readyFlags.isEmpty()) {
      processFlagMessages(table, readyFlags);
    }
  }

  /**
   * Counts the flag-containing DataWritten events in {@code envelopes} and merges those
   * counts into {@link #pendingFlagVotes}. Each DataWritten event represents one source
   * partition's broadcast copy of the flag (one FlagWriterResult produced by the task that
   * processed that partition). The first-seen flag payload for each type is stored in
   * {@link #pendingFlagData}.
   */
  private void accumulateFlagVotes(TableIdentifier tableIdentifier, List<Envelope> envelopes) {
    Map<String, Integer> votesThisCycle =
        Deduplicated.flagMessageVoteCounts(envelopes, this.config.flagTypeField());
    if (votesThisCycle.isEmpty()) {
      return;
    }
    Map<String, Pair<TableContext, Map<String, Object>>> dataThisCycle =
        Deduplicated.flagMessages(commitState.currentCommitId(), tableIdentifier,
            envelopes, this.config.branchesRegexDelimiter(), this.config.flagTypeField());

    Map<String, Integer> votes =
        pendingFlagVotes.computeIfAbsent(tableIdentifier, k -> Maps.newHashMap());
    Map<String, Pair<TableContext, Map<String, Object>>> data =
        pendingFlagData.computeIfAbsent(tableIdentifier, k -> Maps.newHashMap());

    votesThisCycle.forEach((type, count) -> {
      int newTotal = votes.merge(type, count, Integer::sum);
      data.putIfAbsent(type, dataThisCycle.get(type));
      LOG.info("Flag '{}' for table {}: accumulated {}/{} partition votes",
          type, tableIdentifier, newTotal, totalPartitionCount);
    });
  }

  /**
   * Returns and removes all flag types for {@code tableIdentifier} that have accumulated
   * a vote from every source partition (vote count {@code >= totalPartitionCount}).
   * Because the flag is broadcast to all N source partitions, N DataWritten events
   * (one per partition) must arrive before the flag is safe to process.
   */
  private Map<String, Pair<TableContext, Map<String, Object>>> drainReadyFlags(
      TableIdentifier tableIdentifier) {
    Map<String, Integer> votes = pendingFlagVotes.getOrDefault(tableIdentifier, Maps.newHashMap());
    Map<String, Pair<TableContext, Map<String, Object>>> data =
        pendingFlagData.getOrDefault(tableIdentifier, Maps.newHashMap());

    List<String> readyTypes = votes.entrySet().stream()
        .filter(e -> e.getValue() >= totalPartitionCount)
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
          type, tableIdentifier, totalPartitionCount);
    });
    return ready;
  }

  private void processFlagMessages(Table table, Map<String, Pair<TableContext, Map<String, Object>>> flagMessages) {
    flagMessages.forEach((type, flagEntry) -> {
      TableContext flagMessage = flagEntry.first();
      Map<String, Object> flagRecord = flagEntry.second();
      LOG.debug("About to process flag of type {} for: {}, record: {}", type, flagMessage.tableIdentifier().toString(), flagRecordToString(flagRecord));

      switch (type) {
        case "END-LOAD":
          String targetBranch = flagMessage.branch();
          if (targetBranch != null) {
            LOG.info("Processing flag message for table {}, switching to branch {}",
                    table.name(), targetBranch);

            UpdateSchema updateSchemaCommit = table.updateSchema();

            table.schema().columns().stream()
                    .filter(field -> field.name().endsWith("_pending_type_update"))
                    .forEach(field -> {
                      String original = field.name().split("_pending_type_update")[0];

                      updateSchemaCommit.deleteColumn(original).renameColumn(field.name(), original);
                    });

            try {
              updateSchemaCommit.commit();
              LOG.info("Successfully updated types for table {}", table.name());
            } catch (Exception e) {
              LOG.error("Failed to update types for table {}. {}", table.name(), e.getMessage());
            }

            try {
              // Forward the branch: set current snapshot to the branch's snapshot
              // and clear the branch for further use
              table.manageSnapshots().setCurrentSnapshot(table.snapshot(targetBranch).snapshotId()).commit();
              table.manageSnapshots().replaceBranch(targetBranch, table.history().get(0).snapshotId()).commit();
              LOG.info("Successfully switched branch for table {} to {}", table.name(), targetBranch);
            } catch (Exception e) {
              LOG.error("Failed to switch branch for table {} to {}", table.name(), targetBranch, e);
            }
          }
          break;
        case "TYPE-CHANGE":
          UpdateSchema updateSchemaCommit = table.updateSchema();


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

  private String flagRecordToString(Map<String, Object> flagRecord) {
    try {
      return MAPPER.writeValueAsString(flagRecord);
    } catch (Exception e) {
      return String.valueOf(flagRecord);
    }
  }

  @Override
  public void close() throws IOException {
    exec.shutdownNow();
    stop();
  }
}
