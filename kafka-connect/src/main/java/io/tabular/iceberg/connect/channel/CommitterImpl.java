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
  private final SinkTaskContext context;
  private final IcebergSinkConfig config;
  private final Optional<CoordinatorThread> maybeCoordinatorThread;

  /**
   * The commit-ID from the very first {@link org.apache.iceberg.connect.events.StartCommit} that
   * arrived when {@link CommittableSupplier#isPendingFlagCommit()} was still {@code false}.
   *
   * <p><b>Why this is needed:</b> After a task restart the Kafka Connect framework fetches one
   * batch of source records <em>before</em> the {@link SinkTaskContext#offset} rewind (issued in
   * the constructor) takes effect.  That first batch therefore does <em>not</em> contain the flag
   * record.  Concurrently, the coordinator fires its fast-start
   * {@link org.apache.iceberg.connect.events.StartCommit} immediately after being created.  If
   * {@link #commit} calls {@code consumer.poll(Duration.ZERO)} at this moment the StartCommit is
   * consumed and the worker responds with <em>empty</em> flag data — because the flag has not been
   * detected yet.  The flag record then arrives in the <em>second</em> batch (from the rewound
   * offset), {@code isPendingFlagCommit()} flips to {@code true}, but the next StartCommit is
   * {@code commitIntervalMs} away (default five minutes).
   *
   * <p><b>Fix:</b> The very first StartCommit that arrives while
   * {@code isPendingFlagCommit() == false} is <em>deferred</em> by one {@code put()} cycle.  At
   * the top of the <em>next</em> {@link #commit} call the deferred commitId is responded to using
   * the supplied {@link CommittableSupplier} — by which time {@code write()} has already processed
   * the current batch (which may include the flag record), so {@code committable()} returns the
   * {@link io.tabular.iceberg.connect.data.FlagWriterResult}.
   */
  private UUID pendingStartCommitId = null;

  /**
   * Set to {@code true} after {@link #requestImmediateCommit()} has been forwarded to the
   * coordinator for the current flag-commit cycle.  Reset to {@code false} when the flag is
   * resolved (sentinel received) or when the flag-pending state ends, so that the mechanism
   * is available for subsequent flag cycles.
   */
  private boolean immediateCommitTriggered = false;

  /**
   * Set to {@code true} after the first {@link org.apache.iceberg.connect.events.StartCommit} has
   * been seen (whether deferred or responded-to immediately).  Once set, subsequent StartCommits
   * are always responded to without deferral so normal steady-state commit latency is unaffected.
   */
  private boolean firstStartCommitHandled = false;

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

    // Start the coordinator AFTER the initial poll completes.  CommitState now fires the
    // very first StartCommit immediately (no commitIntervalMs wait), so starting the
    // coordinator before the poll would risk having the constructor's empty-committable
    // handler consume that fast-start StartCommit and respond with no flag data.
    // Running the two 1-second initializations sequentially (total ~2 s) is an acceptable
    // trade-off to eliminate this race.
    this.maybeCoordinatorThread = coordinatorThreadFactory.create(context, config);
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
      if (!firstStartCommitHandled && !committableSupplier.isPendingFlagCommit()) {
        // Defer this StartCommit by one put() cycle.  write() will run before the next commit()
        // call, so any flag record in the current batch will be visible in committable() when we
        // respond.  Only the very first StartCommit is deferred; after that normal behaviour
        // resumes so steady-state commit latency is unaffected.
        LOG.info(
            "Deferring first StartCommit {} to next put() cycle (no flag pending yet)",
            commitId);
        pendingStartCommitId = commitId;
      } else {
        sendCommitResponse(commitId, committableSupplier);
      }
      firstStartCommitHandled = true;
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

  /**
   * Poll duration used in {@link #commit} when the worker is paused waiting for the
   * flag-processed sentinel.  A positive duration is required so the Kafka consumer actually
   * waits for the {@link org.apache.iceberg.connect.events.StartCommit} (or sentinel) fetch
   * response instead of returning immediately with zero records.
   *
   * <p>With {@link Duration#ZERO}, {@code consumer.poll()} sends a fetch request to the broker
   * but returns immediately without waiting for the response; the result arrives only on the
   * <em>next</em> call.  When all source-topic partitions are paused Kafka Connect may call
   * {@code put()} very infrequently, so relying on the next call is unreliable.  Using 100 ms
   * here is negligible in the paused state (no source records are being processed) but ensures
   * the consumer receives events in a single {@code commit()} call.
   */
  private static final Duration PENDING_FLAG_POLL_DURATION = Duration.ofMillis(100);

  @Override
  public void commit(CommittableSupplier committableSupplier) {
    throwExceptionIfCoordinatorIsTerminated();

    // Respond to a StartCommit that was deferred from the previous put() cycle.  By the time
    // this runs, write() has already processed the current batch, so committable() will include
    // any FlagWriterResult that was detected in that write() call.
    if (pendingStartCommitId != null) {
      LOG.info("Responding to deferred StartCommit {}", pendingStartCommitId);
      sendCommitResponse(pendingStartCommitId, committableSupplier);
      pendingStartCommitId = null;
    }

    // After a pod crash + restart, the coordinator's fast-start StartCommit can be consumed and
    // responded to with empty data before the rewound flag record arrives (Kafka Connect fetch
    // pipelining).  Without intervention the Worker would wait up to commitIntervalMs for the
    // next StartCommit while paused.  The first time we detect that a flag commit is pending we
    // ask the coordinator (which runs in the same JVM when this task is the leader) to fire a
    // new StartCommit immediately, bypassing the timer.  The flag is broadcast to all source
    // partitions, so the coordinator/leader task always receives it and can trigger itself.
    if (committableSupplier.isPendingFlagCommit()) {
      if (!immediateCommitTriggered) {
        LOG.info("Flag commit pending — requesting immediate StartCommit from coordinator");
        maybeCoordinatorThread.ifPresent(CoordinatorThread::requestImmediateCommit);
        immediateCommitTriggered = true;
      }
    } else {
      // Flag resolved (or never set) — reset so the mechanism is available for the next cycle.
      immediateCommitTriggered = false;
    }

    // Use a positive duration while waiting for the flag sentinel so the consumer doesn't
    // return immediately with 0 records.  In normal operation (not paused) Duration.ZERO
    // keeps overhead negligible.
    Duration pollDuration =
        committableSupplier.isPendingFlagCommit() ? PENDING_FLAG_POLL_DURATION : Duration.ZERO;
    consumeAvailable(pollDuration, envelope -> receive(envelope, committableSupplier));
  }

  @Override
  public void close() throws IOException {
    stop();
    maybeCoordinatorThread.ifPresent(CoordinatorThread::terminate);
  }
}
