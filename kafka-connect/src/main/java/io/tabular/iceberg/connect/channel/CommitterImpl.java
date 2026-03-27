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
import java.util.HashMap;
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
  private final SinkTaskContext context;
  private final IcebergSinkConfig config;
  private final Optional<CoordinatorThread> maybeCoordinatorThread;

  // Pending-flag state detected in committed consumer-group offset metadata on startup.
  // Maps source TopicPartition → (committedOffset, rawTableId).
  // Populated in the constructor; consumed (and cleared) on the first commit() call.
  private final Map<TopicPartition, Long> startupFlagOffsets = new HashMap<>();
  private final Map<TopicPartition, TableIdentifier> startupFlagTables = new HashMap<>();
  private boolean pendingFlagStateRestored = false;

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
    // Fetch the full OffsetAndMetadata so we can inspect per-partition metadata for FLAG_PENDING.
    Map<TopicPartition, OffsetAndMetadata> stableOffsets =
        fetchStableConsumerOffsets(config.controlGroupId());

    Map<TopicPartition, Long> plainOffsets =
        stableOffsets.entrySet().stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().offset()));

    // Rewind kafka connect consumer to avoid duplicates
    context.offset(plainOffsets);

    // Detect any FLAG_PENDING metadata left by a previous run that crashed while a flag was in
    // progress.  We record the info here and restore it in the first commit() call (after the
    // Worker has been fully initialised by TaskImpl).
    stableOffsets.forEach(
        (tp, oam) -> {
          String meta = oam.metadata();
          if (meta != null && meta.startsWith(CommittableSupplier.FLAG_METADATA_PREFIX)) {
            String rawTableId = meta.substring(CommittableSupplier.FLAG_METADATA_PREFIX.length());
            LOG.info(
                "Detected FLAG_PENDING metadata for partition {} table {} at offset {} — "
                    + "will restore paused state on first commit",
                tp, rawTableId, oam.offset());
            startupFlagOffsets.put(tp, oam.offset());
            startupFlagTables.put(tp, TableIdentifier.parse(rawTableId));
          }
        });

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
      CommitToTable commitToTable = (CommitToTable) envelope.event().payload();
      // The Coordinator sends a per-table sentinel CommitToTable (with the all-zeros UUID) after
      // processing all pending flag messages for a specific table.  Only workers routing to that
      // table clear their reroute state and resume; others are unaffected (the Worker checks the
      // table identifier).
      if (Coordinator.FLAG_PROCESSED_SENTINEL_ID.equals(commitToTable.commitId())) {
        committableSupplier.onFlagProcessed(commitToTable.tableReference().identifier());
      }
      return true;
    }
    return false;
  }

  private void sendCommitResponse(UUID commitId, CommittableSupplier committableSupplier) {
    Committable committable = committableSupplier.committable();

    // Obtain any FLAG_PENDING metadata that should be encoded in the committed offsets.
    // This keeps the persistent "flag in progress" state alive in controlGroupId across restarts
    // until onFlagProcessed() clears it on the next cycle.
    Map<TopicPartition, String> flagMeta = committableSupplier.pendingFlagMetadata();

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

    Map<TopicPartition, Offset> offsets = committable.offsetsByTopicPartition();
    send(events, offsets, flagMeta, new ConsumerGroupMetadata(config.controlGroupId()));
    send(ImmutableList.of(), offsets, flagMeta, new ConsumerGroupMetadata(config.connectGroupId()));
  }

  @Override
  public void commit(CommittableSupplier committableSupplier) {
    throwExceptionIfCoordinatorIsTerminated();

    // On the first commit() call after startup, restore any FLAG_PENDING state that was detected
    // from committed consumer-group offset metadata.  This is done here rather than in the
    // constructor because the Worker (CommittableSupplier) is created and fully initialised by
    // TaskImpl before commit() is ever called.
    if (!pendingFlagStateRestored) {
      pendingFlagStateRestored = true;
      startupFlagOffsets.forEach(
          (tp, offset) ->
              committableSupplier.restorePendingFlagState(
                  tp, startupFlagTables.get(tp), offset));
    }

    consumeAvailable(Duration.ZERO, envelope -> receive(envelope, committableSupplier));
  }

  @Override
  public void close() throws IOException {
    stop();
    maybeCoordinatorThread.ifPresent(CoordinatorThread::terminate);
  }
}
