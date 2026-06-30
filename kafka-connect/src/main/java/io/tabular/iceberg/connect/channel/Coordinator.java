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

  private void doCommit(boolean partialCommit) {
    Map<TableIdentifier, Map<UUID, List<Envelope>>> commitMap = commitState.tableCommitMap();

    LOG.info("Commiting the following commits:");

    commitMap.forEach((t, m) -> m.forEach((id, events) -> {
      LOG.info("Commiting {} events at id {} for table {}", events.size(), id, t.toString());
    }));

    String offsetsJson = offsetsJson();
    OffsetDateTime vtts = commitState.vtts(partialCommit);

    Tasks.foreach(commitMap.entrySet())
        .executeWith(exec)
        .stopOnFailure()
        .run(
            entry -> {
              // Commit each commit-id as its own Iceberg snapshot, in first-seen (chronological)
              // order, so equality deletes from a later commit land in a later snapshot (higher
              // sequence number) than the data they must remove. Only the LAST commit-id for the
              // table records the offsets/vtts watermark, so recovery never reads a watermark from
              // an intermediate snapshot that does not yet reflect all data committed in this batch.
              List<UUID> commitIdsInOrder = new ArrayList<>(entry.getValue().keySet());
              int lastIdx = commitIdsInOrder.size() - 1;
              for (int i = 0; i <= lastIdx; i++) {
                UUID commitId = commitIdsInOrder.get(i);
                List<Envelope> envelopes = entry.getValue().get(commitId);
                commitToTable(
                    entry.getKey(),
                    commitId,
                    envelopes,
                    i == lastIdx ? offsetsJson : null,
                    i == lastIdx ? vtts : null);
              }
            });

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

  private void commitToTable(
      TableIdentifier paramTableIdentifier,
      UUID commitId,
      List<Envelope> envelopeList,
      String offsetsJson,
      OffsetDateTime vtts) {
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

    this.tableTopicPartitions.put(
            tableIdentifier.toString(),
            this.members.stream().mapToInt(desc -> (int) desc.assignment().topicPartitions()
                    .stream()
                    .filter(tp ->
                            tp.topic().equals(Deduplicated.extractTopic(filteredEnvelopeList)))
                    .count())
                    .sum()
    );
    accumulateFlagVotes(commitId, tableIdentifier, filteredEnvelopeList);

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

    Map<String, Pair<TableContext, Map<String, Object>>> readyFlags = drainReadyFlags(tableIdentifier);
    if (!readyFlags.isEmpty()) {
      processFlagMessages(table, readyFlags);
      LOG.info(
              "Flags processed for table {} in commit {}, sending per-table resume signal",
              paramTableIdentifier, commitId);
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
