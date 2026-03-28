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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitterImpl extends Channel implements Committer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(CommitterImpl.class);

  // Poll duration for the control topic consumer during commit().
  //
  // committable() is ONLY invoked when a START_COMMIT event is received from the Coordinator
  // on the control topic.  The Coordinator sends START_COMMIT on a periodic timer
  // (commitIntervalMs).  Sending more source-topic records does NOT trigger committable().
  // SinkTaskContext.requestCommit() only tells the Kafka Connect framework to commit its own
  // offsets — it has no effect on the Iceberg commit protocol.
  //
  // BEFORE restart (steady state):
  //   The CommitterImpl consumer has been running continuously.  Its background Kafka fetch
  //   thread keeps the internal record buffer populated with new control-topic messages.
  //   When the Coordinator sends START_COMMIT, it is delivered to the buffer promptly.
  //   Even poll(Duration.ZERO) returns it immediately because the record is already buffered.
  //   committable() is called reliably on every Coordinator cycle.
  //
  // AFTER restart:
  //   A brand-new CommitterImpl consumer is created with a random group ID and
  //   auto.offset.reset=latest.  Two things change:
  //   (a) The consumer's fetch thread has not started yet — poll(Duration.ZERO) returns
  //       nothing even if a START_COMMIT message exists, because no network I/O is
  //       performed.  A non-zero duration (100ms) gives the fetch thread time to deliver
  //       any available records.
  //   (b) More importantly: the Coordinator may also have restarted, so its commit-interval
  //       timer starts fresh — no START_COMMIT has been produced yet.  In this case,
  //       committable() will NOT be called until the timer fires (up to commitIntervalMs
  //       later), regardless of the poll duration.
  //
  // For flag records specifically, drainPendingFlagCommittable() bypasses the START_COMMIT
  // dependency entirely by sending flag results eagerly to the Coordinator.  This is the
  // actual fix for the restart case — the non-zero poll duration only helps with (a) above.
  @VisibleForTesting
  static final Duration COMMIT_POLL_DURATION = Duration.ofMillis(100);

  private final SinkTaskContext context;
  private final IcebergSinkConfig config;
  private final Optional<CoordinatorThread> maybeCoordinatorThread;

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

    // The source-of-truth for source-topic offsets is the control-group-id
    Map<TopicPartition, Long> stableConsumerOffsets =
        fetchStableConsumerOffsets(config.controlGroupId());
    // Rewind kafka connect consumer to avoid duplicates
    context.offset(stableConsumerOffsets);

    consumeAvailable(
        // initial poll with longer duration so the consumer will initialize...
        Duration.ofMillis(1000),
        envelope ->
            receive(
                envelope,
                // CommittableSupplier that always returns empty committables
                () -> new Committable(ImmutableMap.of(), ImmutableList.of())));
  }

  private Map<TopicPartition, Long> fetchStableConsumerOffsets(String groupId) {
    try {
      ListConsumerGroupOffsetsResult response =
          admin()
              .listConsumerGroupOffsets(
                  groupId, new ListConsumerGroupOffsetsOptions().requireStable(true));
      return response.partitionsToOffsetAndMetadata().get().entrySet().stream()
          .filter(entry -> context.assignment().contains(entry.getKey()))
          .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
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
    send(events, offsets, new ConsumerGroupMetadata(config.controlGroupId()));
    send(ImmutableList.of(), offsets, new ConsumerGroupMetadata(config.connectGroupId()));
  }

  @Override
  public void commit(CommittableSupplier committableSupplier) {
    throwExceptionIfCoordinatorIsTerminated();
    consumeAvailable(COMMIT_POLL_DURATION, envelope -> receive(envelope, committableSupplier));

    // After polling the control topic, eagerly send any pending flag results to the Coordinator
    // without waiting for START_COMMIT.
    //
    // Why this is needed:
    // committable() is only invoked when a START_COMMIT event is received.  After a restart,
    // committable() may not be called for up to commitIntervalMs because:
    //   - The Coordinator's commit-interval timer starts fresh, so no START_COMMIT has been
    //     sent yet (the Coordinator also restarts in a pod restart).
    //   - Even if only this task restarted, the constructor's initial 1000ms poll may have
    //     consumed the only available START_COMMIT, and the next one won't arrive until the
    //     Coordinator's timer fires again.
    //
    // Before restart, this eager send is not needed because START_COMMIT events flow
    // regularly and committable() is called on each cycle.  But the eager send is harmless
    // in that case because flagWriterResults will already be empty (included in the regular
    // committable() via sendCommitResponse()).
    //
    // Sending DataWritten events eagerly is safe because the Coordinator buffers them in
    // commitState.addResponse() regardless of whether a commit is in progress, and includes
    // them in the next commit cycle.  No DataComplete is sent here, so the Coordinator does
    // not count this task as "ready" until the normal commit cycle.
    Committable flagCommittable = committableSupplier.drainPendingFlagCommittable();
    if (flagCommittable != null && !flagCommittable.writerResults().isEmpty()) {
      UUID eagerCommitId = UUID.randomUUID();
      LOG.info(
          "Eagerly sending {} flag result(s) with eager-commit-id={}",
          flagCommittable.writerResults().size(),
          eagerCommitId);
      List<Event> events = Lists.newArrayList();
      flagCommittable
          .writerResults()
          .forEach(
              writerResult ->
                  events.add(
                      new Event(
                          config.controlGroupId(),
                          new DataWritten(
                              writerResult.partitionStruct(),
                              eagerCommitId,
                              TableReference.of(
                                  config.catalogName(), writerResult.tableIdentifier()),
                              writerResult.dataFiles(),
                              writerResult.deleteFiles()))));
      // Send without committing any source-topic offsets — flag partition offsets must remain
      // uncommitted so the flag can be re-read if the task restarts before all partitions
      // have reported their flags.
      send(events, ImmutableMap.of(), null);
    }
  }

  @Override
  public void close() throws IOException {
    stop();
    maybeCoordinatorThread.ifPresent(CoordinatorThread::terminate);
  }
}
