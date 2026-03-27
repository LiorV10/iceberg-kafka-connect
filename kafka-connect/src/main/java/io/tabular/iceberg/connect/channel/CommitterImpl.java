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
import static java.util.stream.Collectors.toMap;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.Offset;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitterImpl extends Channel implements Committer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);

  /**
   * Prefix used in the Kafka consumer-group offset metadata field to mark a source partition as
   * paused because of a flag record.  Format: {@code PAUSED:<tableIdentifier>}, e.g.,
   * {@code PAUSED:db.tbl}.  Written atomically with the flag's offset so that the pause state
   * survives pod crashes.
   */
  static final String PAUSED_METADATA_PREFIX = "PAUSED:";

  private final SinkTaskContext context;
  private final IcebergSinkConfig config;
  private final Optional<CoordinatorThread> maybeCoordinatorThread;
  /**
   * Maps each source partition that is currently paused (due to a flag) to the flag's
   * {@link TableIdentifier}.  Populated from Kafka consumer-group offset metadata on startup so
   * that the pause state is restored after a pod crash.  Entries are removed when the coordinator
   * broadcasts the flag-processed sentinel for the corresponding table.
   */
  private final Map<TopicPartition, TableIdentifier> pausedPartitionTables = Maps.newLinkedHashMap();
  /**
   * Last committed offset for each currently-paused partition.  Updated on every commit cycle so
   * that if the coordinator processes a flag after a crash we still have the correct offset to
   * include in the clearing commit without needing an extra admin API call.
   */
  private final Map<TopicPartition, Long> pausedOffsets = Maps.newLinkedHashMap();
  /**
   * Maps source partitions that were <em>just resumed</em> (flag processed) to the last committed
   * offset that was stored in {@link #pausedOffsets}.  These offsets are included in the next
   * commit with <em>empty</em> metadata to overwrite the {@code PAUSED:…} string persisted in
   * Kafka, ensuring that a subsequent crash will NOT incorrectly re-pause the partition.
   */
  private final Map<TopicPartition, Long> clearingOffsets = Maps.newLinkedHashMap();

  public CommitterImpl(SinkTaskContext context, IcebergSinkConfig config, Catalog catalog) {
    this(context, config, catalog, new KafkaClientFactory(config.kafkaProps()));
  }

  private CommitterImpl(
      SinkTaskContext context,
      IcebergSinkConfig config,
      Catalog catalog,
      KafkaClientFactory kafkaClientFactory) {
    this(
        context,
        config,
        kafkaClientFactory,
        new CoordinatorThreadFactoryImpl(catalog, kafkaClientFactory));
  }

  @VisibleForTesting
  CommitterImpl(
      SinkTaskContext context,
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      CoordinatorThreadFactory coordinatorThreadFactory) {
    // pass transient consumer group ID to which we never commit offsets
    super(
        "committer",
        IcebergSinkConfig.DEFAULT_CONTROL_GROUP_PREFIX + UUID.randomUUID(),
        config,
        clientFactory);

    this.context = context;
    this.config = config;

    this.maybeCoordinatorThread = coordinatorThreadFactory.create(context, config);

    // The source-of-truth for source-topic offsets is the control-group-id.
    // We also inspect the offset metadata to restore any "PAUSED:<table>" state that was
    // committed before a pod crash, so the pause survives the restart.
    Map<TopicPartition, OffsetAndMetadata> stableOffsets =
        fetchStableConsumerOffsets(config.controlGroupId());

    // Restore persistent pause state from offset metadata
    stableOffsets.forEach((tp, oam) -> {
      if (!context.assignment().contains(tp)) return;
      String meta = oam.metadata();
      if (meta != null && meta.startsWith(PAUSED_METADATA_PREFIX)) {
        TableIdentifier table = TableIdentifier.parse(meta.substring(PAUSED_METADATA_PREFIX.length()));
        context.pause(tp);
        pausedPartitionTables.put(tp, table);
        LOG.info("Restored persistent pause for partition {} (flag table: {})", tp, table);
      }
    });

    // Rewind kafka connect consumer to avoid duplicates
    Map<TopicPartition, Long> offsetsForRewind = stableOffsets.entrySet().stream()
        .filter(e -> context.assignment().contains(e.getKey()))
        .collect(toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    context.offset(offsetsForRewind);

    consumeAvailable(
        // initial poll with longer duration so the consumer will initialize...
        Duration.ofMillis(1000),
        envelope ->
            receive(
                envelope,
                // CommittableSupplier that always returns empty committables
                () -> new Committable(ImmutableMap.of(), ImmutableList.of())));
  }

  private Map<TopicPartition, OffsetAndMetadata> fetchStableConsumerOffsets(String groupId) {
    try {
      ListConsumerGroupOffsetsResult response =
          admin()
              .listConsumerGroupOffsets(
                  groupId, new ListConsumerGroupOffsetsOptions().requireStable(true));
      return response.partitionsToOffsetAndMetadata().get().entrySet().stream()
          .filter(entry -> context.assignment().contains(entry.getKey()))
          .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    } catch (InterruptedException | ExecutionException e) {
      throw new ConnectException(e);
    }
  }

  private void throwExceptionIfCoordinatorIsTerminated() {
    if (maybeCoordinatorThread.map(CoordinatorThread::isTerminated).orElse(false)) {
      throw new IllegalStateException("Coordinator unexpectedly terminated");
    }
  }

  private boolean receive(Envelope envelope, CommittableSupplier committableSupplier) {
    if (envelope.event().type() == PayloadType.START_COMMIT) {
      UUID commitId = ((StartCommit) envelope.event().payload()).commitId();
      sendCommitResponse(commitId, committableSupplier);
      return true;
    }
    if (envelope.event().type() == PayloadType.COMMIT_TO_TABLE) {
      CommitToTable payload = (CommitToTable) envelope.event().payload();
      // The coordinator broadcasts CommitToTable with the FLAG_PROCESSED_SENTINEL_ID after it
      // has handled all flag events for a specific table.  Use it to resume the paused partitions.
      if (Coordinator.FLAG_PROCESSED_SENTINEL_ID.equals(payload.commitId())) {
        TableIdentifier table = payload.tableReference().identifier();
        resumeFlagPartitions(table, committableSupplier);
      }
      return true;
    }
    return false;
  }

  /**
   * Resumes all partitions that were paused for {@code flagTable}, schedules one clearing commit
   * to overwrite the {@code PAUSED:…} metadata with an empty string, and notifies the
   * {@link CommittableSupplier} so that the Worker can update its own internal state.
   */
  private void resumeFlagPartitions(TableIdentifier flagTable, CommittableSupplier supplier) {
    List<TopicPartition> toResume =
        pausedPartitionTables.entrySet().stream()
            .filter(e -> e.getValue().equals(flagTable))
            .map(Map.Entry::getKey)
            .collect(toList());
    if (toResume.isEmpty()) {
      return;
    }
    toResume.forEach(tp -> {
      // Carry over the stored offset so clearingOffsets never needs an admin API call.
      Long lastOffset = pausedOffsets.remove(tp);
      clearingOffsets.put(tp, lastOffset != null ? lastOffset : 0L);
      pausedPartitionTables.remove(tp);
    });
    supplier.onFlagProcessed(flagTable);
    LOG.info(
        "Flag processed for table {}; scheduled metadata clearing for {} partition(s)",
        flagTable,
        toResume.size());
  }

  private void sendCommitResponse(UUID commitId, CommittableSupplier committableSupplier) {
    Committable committable = committableSupplier.committable();

    List<Event> events = Lists.newArrayList();

    committable
        .writerResults()
        .forEach(
            writerResult -> {
              Event commitResponse =
                  new Event(
                      config.controlGroupId(),
                      new DataWritten(
                          writerResult.partitionStruct(),
                          commitId,
                          TableReference.of(config.catalogName(), writerResult.tableIdentifier()),
                          writerResult.dataFiles(),
                          writerResult.deleteFiles()));

              events.add(commitResponse);
            });

    // include all assigned topic partitions even if no messages were read
    // from a partition, as the coordinator will use that to determine
    // when all data for a commit has been received
    List<TopicPartitionOffset> assignments =
        context.assignment().stream()
            .map(
                topicPartition -> {
                  Offset offset =
                      committable.offsetsByTopicPartition().getOrDefault(topicPartition, null);
                  return new TopicPartitionOffset(
                      topicPartition.topic(),
                      topicPartition.partition(),
                      offset == null ? null : offset.offset(),
                      offset == null ? null : offset.timestamp());
                })
            .collect(toList());

    Event commitReady =
        new Event(
            config.controlGroupId(),
            new DataComplete(commitId, assignments));
    events.add(commitReady);

    // Build the OffsetAndMetadata map.
    // - Partitions currently paused for a flag: commit with "PAUSED:<table>" metadata so the pause
    //   state is persisted atomically with the DataWritten event in the same Kafka transaction.
    // - Partitions that were just resumed (clearing): commit with empty metadata to overwrite the
    //   stale "PAUSED:…" string, preventing an incorrect re-pause after a future crash.
    // - All other partitions: commit with empty metadata (normal behaviour).
    Map<TopicPartition, OffsetAndMetadata> controlGroupOffsets = Maps.newLinkedHashMap();
    committable.offsetsByTopicPartition().forEach((tp, offset) ->
        controlGroupOffsets.put(tp, new OffsetAndMetadata(offset.offset())));

    // Update in-memory paused table tracking from the latest committable, then apply PAUSED metadata
    committable.pausedPartitionTables().forEach(pausedPartitionTables::putIfAbsent);
    pausedPartitionTables.forEach((tp, table) -> {
      long offset = controlGroupOffsets.containsKey(tp)
          ? controlGroupOffsets.get(tp).offset()
          : pausedOffsets.getOrDefault(tp, 0L);
      pausedOffsets.put(tp, offset); // keep the latest offset for use in clearingOffsets later
      controlGroupOffsets.put(tp, new OffsetAndMetadata(offset, PAUSED_METADATA_PREFIX + table));
    });

    // Clearing: write empty metadata (overwriting PAUSED:…) for just-resumed partitions.
    // clearingOffsets already contains the stored offset (from pausedOffsets at resume time)
    // so no additional admin API call is needed.
    clearingOffsets.forEach((tp, storedOffset) -> {
      if (!controlGroupOffsets.containsKey(tp)) {
        controlGroupOffsets.put(tp, new OffsetAndMetadata(storedOffset));
      }
      // If it IS in controlGroupOffsets already (new record arrived), metadata is already "" - good
    });

    sendWithOffsetMetadata(events, controlGroupOffsets, new ConsumerGroupMetadata(config.controlGroupId()));

    // Also commit to the connect consumer group (plain offsets, no metadata needed)
    Map<TopicPartition, Offset> connectOffsets = committable.offsetsByTopicPartition();
    send(ImmutableList.of(), connectOffsets, new ConsumerGroupMetadata(config.connectGroupId()));

    // After successful sends, clear the clearingOffsets (PAUSED metadata is now overwritten)
    clearingOffsets.clear();
  }

  @Override
  public void commit(CommittableSupplier committableSupplier) {
    throwExceptionIfCoordinatorIsTerminated();
    consumeAvailable(Duration.ZERO, envelope -> receive(envelope, committableSupplier));
  }

  @Override
  public void close() throws IOException {
    stop();
    maybeCoordinatorThread.ifPresent(CoordinatorThread::terminate);
  }
}
