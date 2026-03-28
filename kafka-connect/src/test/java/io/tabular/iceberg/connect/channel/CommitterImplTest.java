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

import static io.tabular.iceberg.connect.fixtures.EventTestUtil.createDataFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.WriterResult;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class CommitterImplTest {

  private static final String CATALOG_NAME = "iceberg";
  private static final String SOURCE_TOPIC = "source-topic-name";
  private static final TopicPartition SOURCE_TP0 = new TopicPartition(SOURCE_TOPIC, 0);
  private static final TopicPartition SOURCE_TP1 = new TopicPartition(SOURCE_TOPIC, 1);
  // note: only partition=0 is assigned
  private static final Set<TopicPartition> ASSIGNED_SOURCE_TOPIC_PARTITIONS =
      ImmutableSet.of(SOURCE_TP0);
  private static final String CONNECTOR_NAME = "connector-name";
  private static final String TABLE_1_NAME = "db.tbl1";
  private static final TableIdentifier TABLE_1_IDENTIFIER = TableIdentifier.parse(TABLE_1_NAME);
  private static final String CONTROL_TOPIC = "control-topic-name";
  private static final TopicPartition CONTROL_TOPIC_PARTITION =
      new TopicPartition(CONTROL_TOPIC, 0);
  private KafkaClientFactory kafkaClientFactory;
  private UUID producerId;
  private MockProducer<String, byte[]> producer;
  private MockConsumer<String, byte[]> consumer;
  private Admin admin;

  @BeforeEach
  public void before() {
    admin = mock(Admin.class);

    producerId = UUID.randomUUID();
    producer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
    producer.initTransactions();

    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    kafkaClientFactory = mock(KafkaClientFactory.class);
    when(kafkaClientFactory.createConsumer(any())).thenReturn(consumer);
    when(kafkaClientFactory.createProducer(any())).thenReturn(Pair.of(producerId, producer));
    when(kafkaClientFactory.createAdmin()).thenReturn(admin);
  }

  @AfterEach
  public void after() {
    producer.close();
    consumer.close();
    admin.close();
  }

  private void initConsumer() {
    consumer.rebalance(ImmutableList.of(CONTROL_TOPIC_PARTITION));
    consumer.updateBeginningOffsets(ImmutableMap.of(CONTROL_TOPIC_PARTITION, 0L));
  }

  private static IcebergSinkConfig makeConfig(int taskId) {
    return new IcebergSinkConfig(
        ImmutableMap.of(
            "name",
            CONNECTOR_NAME,
            "iceberg.catalog.catalog-impl",
            "org.apache.iceberg.inmemory.InMemoryCatalog",
            "iceberg.tables",
            TABLE_1_NAME,
            "iceberg.control.topic",
            CONTROL_TOPIC,
            IcebergSinkConfig.INTERNAL_TRANSACTIONAL_SUFFIX_PROP,
            "-txn-" + UUID.randomUUID() + "-" + taskId));
  }

  private static final IcebergSinkConfig CONFIG = makeConfig(1);

  private SinkTaskContext mockContext() {
    SinkTaskContext mockContext = mock(SinkTaskContext.class);
    when(mockContext.assignment()).thenReturn(ASSIGNED_SOURCE_TOPIC_PARTITIONS);
    return mockContext;
  }

  private static DynConstructors.Ctor<CoordinatorKey> ctorCoordinatorKey() {
    return DynConstructors.builder(CoordinatorKey.class)
        .hiddenImpl(
            "org.apache.kafka.clients.admin.internals.CoordinatorKey",
            FindCoordinatorRequest.CoordinatorType.class,
            String.class)
        .build();
  }

  private static DynConstructors.Ctor<ListConsumerGroupOffsetsResult>
      ctorListConsumerGroupOffsetsResult() {
    return DynConstructors.builder(ListConsumerGroupOffsetsResult.class)
        .hiddenImpl("org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult", Map.class)
        .build();
  }

  private final CoordinatorKey coordinatorKey =
      ctorCoordinatorKey()
          .newInstance(FindCoordinatorRequest.CoordinatorType.GROUP, "fakeCoordinatorKey");

  @SuppressWarnings("deprecation")
  private static ListConsumerGroupOffsetsOptions listOffsetResultMatcher() {
    return argThat(x -> x.topicPartitions() == null && x.requireStable());
  }

  private ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult(
      Map<TopicPartition, Long> consumerOffsets) {
    return ctorListConsumerGroupOffsetsResult()
        .newInstance(
            ImmutableMap.of(
                coordinatorKey,
                KafkaFuture.completedFuture(
                    consumerOffsets.entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue()))))));
  }

  private void whenAdminListConsumerGroupOffsetsThenReturn(
      Map<String, Map<TopicPartition, Long>> consumersOffsets) {
    consumersOffsets.forEach(
        (consumerGroup, consumerOffsets) -> {
          when(admin.listConsumerGroupOffsets(eq(consumerGroup), listOffsetResultMatcher()))
              .thenReturn(listConsumerGroupOffsetsResult(consumerOffsets));
        });
  }

  private static class NoOpCoordinatorThreadFactory implements CoordinatorThreadFactory {
    int numTimesCalled = 0;

    @Override
    public Optional<CoordinatorThread> create(SinkTaskContext context, IcebergSinkConfig config) {
      numTimesCalled += 1;
      CoordinatorThread mockThread = mock(CoordinatorThread.class);
      Mockito.doNothing().when(mockThread).start();
      Mockito.doNothing().when(mockThread).terminate();
      return Optional.of(mockThread);
    }
  }

  private static class TerminatedCoordinatorThreadFactory implements CoordinatorThreadFactory {
    @Override
    public Optional<CoordinatorThread> create(SinkTaskContext context, IcebergSinkConfig config) {
      CoordinatorThread mockThread = mock(CoordinatorThread.class);
      Mockito.doNothing().when(mockThread).start();
      Mockito.doNothing().when(mockThread).terminate();
      Mockito.doReturn(true).when(mockThread).isTerminated();
      return Optional.of(mockThread);
    }
  }

  private static <F> String toPath(ContentFile<F> contentFile) {
    return contentFile.path().toString();
  }

  private static <F extends ContentFile<F>> void assertSameContentFiles(
      List<F> actual, List<F> expected) {
    assertThat(actual.stream().map(CommitterImplTest::toPath).collect(Collectors.toList()))
        .containsExactlyElementsOf(
            expected.stream().map(CommitterImplTest::toPath).collect(Collectors.toList()));
  }

  private void assertDataWritten(
      ProducerRecord<String, byte[]> producerRecord,
      UUID expectedProducerId,
      UUID expectedCommitId,
      TableIdentifier expectedTableIdentifier,
      List<DataFile> expectedDataFiles,
      List<DeleteFile> expectedDeleteFiles) {
    assertThat(producerRecord.key()).isEqualTo(expectedProducerId.toString());

    Event event = AvroUtil.decode(producerRecord.value());
    assertThat(event.type()).isEqualTo(PayloadType.DATA_WRITTEN);
    assertThat(event.payload()).isInstanceOf(DataWritten.class);
    DataWritten payload = (DataWritten) event.payload();
    assertThat(payload.commitId()).isEqualTo(expectedCommitId);
    assertThat(payload.tableReference().identifier()).isEqualTo(expectedTableIdentifier);
    assertThat(payload.tableReference().catalog()).isEqualTo(CATALOG_NAME);
    assertSameContentFiles(payload.dataFiles(), expectedDataFiles);
    assertSameContentFiles(payload.deleteFiles(), expectedDeleteFiles);
  }

  private void assertDataComplete(
      ProducerRecord<String, byte[]> producerRecord,
      UUID expectedProducerId,
      UUID expectedCommitId,
      Map<TopicPartition, Pair<Long, OffsetDateTime>> expectedAssignments) {
    assertThat(producerRecord.key()).isEqualTo(expectedProducerId.toString());

    Event event = AvroUtil.decode(producerRecord.value());
    assertThat(event.type()).isEqualTo(PayloadType.DATA_COMPLETE);
    assertThat(event.payload()).isInstanceOf(DataComplete.class);
    DataComplete commitReadyPayload = (DataComplete) event.payload();
    assertThat(commitReadyPayload.commitId()).isEqualTo(expectedCommitId);
    assertThat(
            commitReadyPayload.assignments().stream()
                .map(
                    x ->
                        Pair.of(
                            new TopicPartition(x.topic(), x.partition()),
                            Pair.of(x.offset(), x.timestamp())))
                .collect(Collectors.toList()))
        .isEqualTo(
            expectedAssignments.entrySet().stream()
                .map(e -> Pair.of(e.getKey(), e.getValue()))
                .collect(Collectors.toList()));
  }

  private OffsetDateTime offsetDateTime(Long ms) {
   return OffsetDateTime.ofInstant(Instant.ofEpochMilli(ms), ZoneOffset.UTC);
  }

  @Test
  public void
      testShouldRewindOffsetsToStableControlGroupConsumerOffsetsForAssignedPartitionsOnConstruction()
          throws IOException {
    SinkTaskContext mockContext = mockContext();

    ArgumentCaptor<Map<TopicPartition, Long>> offsetArgumentCaptor =
        ArgumentCaptor.forClass(Map.class);

    IcebergSinkConfig config = makeConfig(1);

    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            config.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L),
            config.connectGroupId(), ImmutableMap.of(SOURCE_TP0, 90L, SOURCE_TP1, 80L)));

    try (CommitterImpl ignored =
        new CommitterImpl(mockContext, config, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();

      verify(mockContext).offset(offsetArgumentCaptor.capture());
      assertThat(offsetArgumentCaptor.getAllValues())
          .isEqualTo(ImmutableList.of(ImmutableMap.of(SOURCE_TP0, 110L)));
    }
  }

  @Test
  public void testCommitShouldThrowExceptionIfCoordinatorIsTerminated() throws IOException {
    SinkTaskContext mockContext = mockContext();
    IcebergSinkConfig config = makeConfig(0);

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            config.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    TerminatedCoordinatorThreadFactory coordinatorThreadFactory =
        new TerminatedCoordinatorThreadFactory();

    CommittableSupplier committableSupplier =
        () -> {
          throw new NotImplementedException("Should not be called");
        };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, config, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      assertThatThrownBy(() -> committer.commit(committableSupplier))
          .isInstanceOf(RuntimeException.class)
          .hasMessage("Coordinator unexpectedly terminated");

      assertThat(producer.history()).isEmpty();
      assertThat(producer.consumerGroupOffsetsHistory()).isEmpty();
    }
  }

  @Test
  public void testCommitShouldDoNothingIfThereAreNoMessages() throws IOException {
    SinkTaskContext mockContext = mockContext();

    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    CommittableSupplier committableSupplier =
        () -> {
          throw new NotImplementedException("Should not be called");
        };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      committer.commit(committableSupplier);

      assertThat(producer.history()).isEmpty();
      assertThat(producer.consumerGroupOffsetsHistory()).isEmpty();
    }
  }

  @Test
  public void testCommitShouldDoNothingIfThereIsNoCommitRequestMessage() throws IOException {
    SinkTaskContext mockContext = mockContext();

    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    CommittableSupplier committableSupplier =
        () -> {
          throw new NotImplementedException("Should not be called");
        };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      consumer.addRecord(
          new ConsumerRecord<>(
              CONTROL_TOPIC,
              CONTROL_TOPIC_PARTITION.partition(),
              0,
              UUID.randomUUID().toString(),
              AvroUtil.encode(
                  new Event(
                      CONFIG.controlGroupId(),
                      new CommitComplete(UUID.randomUUID(), offsetDateTime(100L))))));

      committer.commit(committableSupplier);

      assertThat(producer.history()).isEmpty();
      assertThat(producer.consumerGroupOffsetsHistory()).isEmpty();
    }
  }

  @Test
  public void testCommitShouldRespondToCommitRequest() throws IOException {
    SinkTaskContext mockContext = mockContext();

    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();
    UUID commitId = UUID.randomUUID();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    List<DataFile> dataFiles = ImmutableList.of(createDataFile());
    List<DeleteFile> deleteFiles = ImmutableList.of();
    Types.StructType partitionStruct = Types.StructType.of();
    Map<TopicPartition, Offset> sourceOffsets = ImmutableMap.of(SOURCE_TP0, new Offset(100L, 200L));
    CommittableSupplier committableSupplier =
        () ->
            new Committable(
                sourceOffsets,
                ImmutableList.of(
                    new WriterResult(TABLE_1_IDENTIFIER, dataFiles, deleteFiles, partitionStruct)));

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      consumer.addRecord(
          new ConsumerRecord<>(
              CONTROL_TOPIC_PARTITION.topic(),
              CONTROL_TOPIC_PARTITION.partition(),
              0,
              UUID.randomUUID().toString(),
              AvroUtil.encode(
                  new Event(
                      CONFIG.controlGroupId(),
                      new StartCommit(commitId)))));

      committer.commit(committableSupplier);

      assertThat(producer.transactionCommitted()).isTrue();
      assertThat(producer.history()).hasSize(2);
      assertDataWritten(
          producer.history().get(0),
          producerId,
          commitId,
          TABLE_1_IDENTIFIER,
          dataFiles,
          deleteFiles);
      assertDataComplete(
          producer.history().get(1),
          producerId,
          commitId,
          ImmutableMap.of(SOURCE_TP0, Pair.of(100L, offsetDateTime(200L))));

      assertThat(producer.consumerGroupOffsetsHistory()).hasSize(2);
      Map<TopicPartition, OffsetAndMetadata> expectedConsumerOffset =
          ImmutableMap.of(SOURCE_TP0, new OffsetAndMetadata(100L));
      assertThat(producer.consumerGroupOffsetsHistory().get(0))
          .isEqualTo(ImmutableMap.of(CONFIG.controlGroupId(), expectedConsumerOffset));
      assertThat(producer.consumerGroupOffsetsHistory().get(1))
          .isEqualTo(ImmutableMap.of(CONFIG.connectGroupId(), expectedConsumerOffset));
    }
  }

  @Test
  public void testCommitWhenCommittableIsEmpty() throws IOException {
    SinkTaskContext mockContext = mockContext();

    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    UUID commitId = UUID.randomUUID();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    CommittableSupplier committableSupplier =
        () -> new Committable(ImmutableMap.of(), ImmutableList.of());

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      consumer.addRecord(
          new ConsumerRecord<>(
              CONTROL_TOPIC_PARTITION.topic(),
              CONTROL_TOPIC_PARTITION.partition(),
              0,
              UUID.randomUUID().toString(),
              AvroUtil.encode(
                  new Event(
                      CONFIG.controlGroupId(),
                      new StartCommit(commitId)))));


      committer.commit(committableSupplier);

      assertThat(producer.transactionCommitted()).isTrue();
      assertThat(producer.history()).hasSize(1);
      assertDataComplete(
          producer.history().get(0),
          producerId,
          commitId,
          ImmutableMap.of(SOURCE_TP0, Pair.of(null, null)));

      assertThat(producer.consumerGroupOffsetsHistory()).hasSize(0);
    }
  }

  @Test
  public void testCommitShouldCommitOffsetsOnlyForPartitionsWeMadeProgressOn() throws IOException {
    SinkTaskContext mockContext = mockContext();

    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    TopicPartition sourceTp0 = new TopicPartition(SOURCE_TOPIC, 0);
    TopicPartition sourceTp1 = new TopicPartition(SOURCE_TOPIC, 1);
    Set<TopicPartition> sourceTopicPartitions = ImmutableSet.of(sourceTp0, sourceTp1);

    when(mockContext.assignment()).thenReturn(sourceTopicPartitions);

    UUID commitId = UUID.randomUUID();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    List<DataFile> dataFiles = ImmutableList.of(createDataFile());
    List<DeleteFile> deleteFiles = ImmutableList.of();
    Types.StructType partitionStruct = Types.StructType.of();
    CommittableSupplier committableSupplier =
        () ->
            new Committable(
                ImmutableMap.of(sourceTp1, new Offset(100L, 200L)),
                ImmutableList.of(
                    new WriterResult(TABLE_1_IDENTIFIER, dataFiles, deleteFiles, partitionStruct)));

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      consumer.addRecord(
          new ConsumerRecord<>(
              CONTROL_TOPIC_PARTITION.topic(),
              CONTROL_TOPIC_PARTITION.partition(),
              0,
              UUID.randomUUID().toString(),
              AvroUtil.encode(
                  new Event(
                      CONFIG.controlGroupId(),
                      new StartCommit(commitId)))));

      committer.commit(committableSupplier);

      assertThat(producer.transactionCommitted()).isTrue();
      assertThat(producer.history()).hasSize(2);
      assertDataWritten(
          producer.history().get(0),
          producerId,
          commitId,
          TABLE_1_IDENTIFIER,
          dataFiles,
          deleteFiles);
      assertDataComplete(
          producer.history().get(1),
          producerId,
          commitId,
          ImmutableMap.of(
              sourceTp0, Pair.of(null, null),
              sourceTp1, Pair.of(100L, offsetDateTime(200L))));

      assertThat(producer.consumerGroupOffsetsHistory()).hasSize(2);
      Map<TopicPartition, OffsetAndMetadata> expectedConsumerOffset =
          ImmutableMap.of(sourceTp1, new OffsetAndMetadata(100L));
      assertThat(producer.consumerGroupOffsetsHistory().get(0))
          .isEqualTo(ImmutableMap.of(CONFIG.controlGroupId(), expectedConsumerOffset));
      assertThat(producer.consumerGroupOffsetsHistory().get(1))
          .isEqualTo(ImmutableMap.of(CONFIG.connectGroupId(), expectedConsumerOffset));
    }
  }

  /**
   * Verifies that {@link CommitterImpl} calls {@link CommittableSupplier#onFlagProcessed} when
   * it receives a per-table sentinel {@link org.apache.iceberg.connect.events.CommitToTable} event
   * (commit-ID == all-zeros UUID) from the Coordinator.  The sentinel is sent by the Coordinator
   * only after it has collected flag votes from every source partition for that specific table and
   * executed the flag action.  Workers for other tables are unaffected because {@link Worker}
   * checks the table identifier.
   */
  @Test
  public void testOnFlagProcessedIsCalledOnSentinelCommitToTable() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    TableIdentifier[] receivedTableId = {null};
    CommittableSupplier committableSupplier = new CommittableSupplier() {
      @Override
      public Committable committable() {
        return new Committable(ImmutableMap.of(), ImmutableList.of());
      }

      @Override
      public void onFlagProcessed(TableIdentifier tableIdentifier) {
        receivedTableId[0] = tableIdentifier;
      }
    };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      // Deliver the per-table sentinel CommitToTable (all-zeros UUID) as if broadcast by the
      // Coordinator after processing TABLE_1's flag.
      consumer.addRecord(
          new ConsumerRecord<>(
              CONTROL_TOPIC_PARTITION.topic(),
              CONTROL_TOPIC_PARTITION.partition(),
              0,
              UUID.randomUUID().toString(),
              AvroUtil.encode(
                  new Event(
                      CONFIG.controlGroupId(),
                      new CommitToTable(
                          Coordinator.FLAG_PROCESSED_SENTINEL_ID,
                          TableReference.of(CATALOG_NAME, TABLE_1_IDENTIFIER),
                          0L,
                          null)))));

      committer.commit(committableSupplier);

      assertThat(receivedTableId[0])
          .as("onFlagProcessed() must be called with TABLE_1_IDENTIFIER when per-table sentinel received")
          .isEqualTo(TABLE_1_IDENTIFIER);
    }
  }

  /**
   * Verifies that a regular (non-sentinel) {@link org.apache.iceberg.connect.events.CommitToTable}
   * does NOT trigger {@link CommittableSupplier#onFlagProcessed}.
   */
  @Test
  public void testOnFlagProcessedIsNotCalledOnRegularCommitToTable() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    boolean[] onFlagProcessedCalled = {false};
    CommittableSupplier committableSupplier = new CommittableSupplier() {
      @Override
      public Committable committable() {
        return new Committable(ImmutableMap.of(), ImmutableList.of());
      }

      @Override
      public void onFlagProcessed(TableIdentifier tableIdentifier) {
        onFlagProcessedCalled[0] = true;
      }
    };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      // Deliver a regular CommitToTable (non-sentinel UUID).
      consumer.addRecord(
          new ConsumerRecord<>(
              CONTROL_TOPIC_PARTITION.topic(),
              CONTROL_TOPIC_PARTITION.partition(),
              0,
              UUID.randomUUID().toString(),
              AvroUtil.encode(
                  new Event(
                      CONFIG.controlGroupId(),
                      new CommitToTable(
                          UUID.randomUUID(),
                          TableReference.of(CATALOG_NAME, TABLE_1_IDENTIFIER),
                          12345L,
                          null)))));

      committer.commit(committableSupplier);

      assertThat(onFlagProcessedCalled[0])
          .as("onFlagProcessed() must NOT be called for a regular CommitToTable")
          .isFalse();
    }
  }

  /**
   * Verifies that pending flag results are sent eagerly to the control topic as
   * {@link DataWritten} events even when no {@link StartCommit} has been received.
   * This is critical after a task restart: the flag is re-read (offset not committed),
   * but the Coordinator may not send {@code START_COMMIT} for up to {@code commitIntervalMs},
   * so the flag result must be sent immediately via {@code drainPendingFlagCommittable()}.
   */
  @Test
  public void testPendingFlagResultsSentEagerlyWithoutStartCommit() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    List<DataFile> flagDataFiles = ImmutableList.of(createDataFile());
    Types.StructType partitionStruct = Types.StructType.of();

    // CommittableSupplier whose committable() must NOT be called (no START_COMMIT)
    // but drainPendingFlagCommittable() returns a pending flag result.
    boolean[] committableCalled = {false};
    boolean[] drainCalled = {false};
    CommittableSupplier committableSupplier = new CommittableSupplier() {
      @Override
      public Committable committable() {
        committableCalled[0] = true;
        return new Committable(ImmutableMap.of(), ImmutableList.of());
      }

      @Override
      public Committable drainPendingFlagCommittable() {
        if (drainCalled[0]) {
          return null; // second call returns null (already drained)
        }
        drainCalled[0] = true;
        return new Committable(
            ImmutableMap.of(),
            ImmutableList.of(
                new WriterResult(
                    TABLE_1_IDENTIFIER, flagDataFiles, ImmutableList.of(), partitionStruct)));
      }
    };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      // No START_COMMIT on the control topic — just call commit()
      committer.commit(committableSupplier);

      // committable() must NOT have been called (no START_COMMIT)
      assertThat(committableCalled[0])
          .as("committable() must not be called when no START_COMMIT is received")
          .isFalse();

      // drainPendingFlagCommittable() must have been called
      assertThat(drainCalled[0])
          .as("drainPendingFlagCommittable() must be called on every commit()")
          .isTrue();

      // The flag result must have been sent eagerly as a DataWritten event
      assertThat(producer.transactionCommitted()).isTrue();
      assertThat(producer.history()).hasSize(1);

      Event event = AvroUtil.decode(producer.history().get(0).value());
      assertThat(event.type()).isEqualTo(PayloadType.DATA_WRITTEN);
      DataWritten payload = (DataWritten) event.payload();
      assertThat(payload.tableReference().identifier()).isEqualTo(TABLE_1_IDENTIFIER);
      assertSameContentFiles(payload.dataFiles(), flagDataFiles);

      // No source-topic offsets must be committed (flag offsets stay uncommitted)
      assertThat(producer.consumerGroupOffsetsHistory()).isEmpty();
    }
  }

  /**
   * Verifies that when drainPendingFlagCommittable() returns null (no pending flags),
   * no eager send is performed.
   */
  @Test
  public void testNoPendingFlagResultsMeansNoEagerSend() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    CommittableSupplier committableSupplier = new CommittableSupplier() {
      @Override
      public Committable committable() {
        throw new NotImplementedException("Should not be called");
      }

      @Override
      public Committable drainPendingFlagCommittable() {
        return null; // no pending flags
      }
    };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      committer.commit(committableSupplier);

      assertThat(producer.history()).isEmpty();
      assertThat(producer.consumerGroupOffsetsHistory()).isEmpty();
    }
  }

  /**
   * Simulates the restart scenario that explains why committable() is not called after a pod
   * restart, and verifies that drainPendingFlagCommittable() correctly handles it.
   *
   * <p><strong>Before restart (steady state):</strong> The Coordinator sends
   * {@code START_COMMIT} on a regular timer. The CommitterImpl consumer picks it up,
   * {@code sendCommitResponse()} calls {@code committable()}, and the flag result is included.
   *
   * <p><strong>After restart:</strong> The CommitterImpl consumer is brand new (random group,
   * auto.offset.reset=latest). The Coordinator also restarted, so its timer starts fresh —
   * no {@code START_COMMIT} has been produced yet. {@code committable()} is never called.
   * Without the eager send, the flag result would be stranded in the Worker forever.
   *
   * <p>This test verifies:
   * <ol>
   *   <li>{@code committable()} is NOT called (no START_COMMIT)</li>
   *   <li>{@code drainPendingFlagCommittable()} IS called and the flag result IS sent</li>
   *   <li>On a subsequent call (after START_COMMIT arrives), {@code committable()} is called
   *       but the flag result is not duplicated</li>
   * </ol>
   */
  @Test
  public void testRestartScenarioCommittableNotCalledButFlagSentEagerly() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    List<DataFile> flagDataFiles = ImmutableList.of(createDataFile());
    Types.StructType partitionStruct = Types.StructType.of();

    // Track whether each method is called and how many times
    int[] committableCallCount = {0};
    int[] drainCallCount = {0};
    CommittableSupplier committableSupplier = new CommittableSupplier() {
      @Override
      public Committable committable() {
        committableCallCount[0]++;
        return new Committable(ImmutableMap.of(), ImmutableList.of());
      }

      @Override
      public Committable drainPendingFlagCommittable() {
        drainCallCount[0]++;
        if (drainCallCount[0] == 1) {
          // First call: return the pending flag result (simulating flag re-read after restart)
          return new Committable(
              ImmutableMap.of(),
              ImmutableList.of(
                  new WriterResult(
                      TABLE_1_IDENTIFIER, flagDataFiles, ImmutableList.of(), partitionStruct)));
        }
        return null; // subsequent calls: already drained
      }
    };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      // === First commit() call: simulates immediately after restart ===
      // No START_COMMIT on the control topic (Coordinator also restarted, timer not yet fired)
      committer.commit(committableSupplier);

      // committable() must NOT have been called (no START_COMMIT available)
      assertThat(committableCallCount[0])
          .as("committable() must not be called when no START_COMMIT is available after restart")
          .isZero();

      // drainPendingFlagCommittable() must have been called
      assertThat(drainCallCount[0])
          .as("drainPendingFlagCommittable() must be called to eagerly send flag results")
          .isEqualTo(1);

      // The flag result must have been sent eagerly
      assertThat(producer.transactionCommitted()).isTrue();
      assertThat(producer.history()).hasSize(1);
      Event event = AvroUtil.decode(producer.history().get(0).value());
      assertThat(event.type()).isEqualTo(PayloadType.DATA_WRITTEN);

      // No offsets committed (flag offsets must stay uncommitted for re-read)
      assertThat(producer.consumerGroupOffsetsHistory()).isEmpty();

      // Clear producer history for the next assertion
      producer.clear();

      // === Second commit() call: simulates after START_COMMIT eventually arrives ===
      UUID commitId = UUID.randomUUID();
      consumer.addRecord(
          new ConsumerRecord<>(
              CONTROL_TOPIC_PARTITION.topic(),
              CONTROL_TOPIC_PARTITION.partition(),
              0,
              UUID.randomUUID().toString(),
              AvroUtil.encode(
                  new Event(
                      CONFIG.controlGroupId(),
                      new StartCommit(commitId)))));

      committer.commit(committableSupplier);

      // NOW committable() must have been called (START_COMMIT received)
      assertThat(committableCallCount[0])
          .as("committable() must be called when START_COMMIT is received")
          .isEqualTo(1);

      // drainPendingFlagCommittable() is called again but returns null (already drained)
      assertThat(drainCallCount[0])
          .as("drainPendingFlagCommittable() is called on every commit()")
          .isEqualTo(2);

      // Only DataComplete should be sent (no flag result — already drained in first call)
      assertThat(producer.history()).hasSize(1);
      Event completeEvent = AvroUtil.decode(producer.history().get(0).value());
      assertThat(completeEvent.type()).isEqualTo(PayloadType.DATA_COMPLETE);
    }
  }

  /**
   * Verifies the fix for the deadlock after a pod restart: when ALL source partitions are paused
   * (because the flag was broadcast to every partition), the polling loop in {@code commit()}
   * keeps consuming the control topic until the per-table sentinel is received and partitions
   * are unpaused.
   *
   * <p>Without this fix, Kafka Connect stops calling {@code put()} when all partitions are
   * paused, so {@code commit()} would never be called again, and the {@code onFlagProcessed}
   * sentinel would never be received — creating a permanent deadlock.
   *
   * <p>This test adds the sentinel to the control topic before calling commit(). The sentinel
   * is consumed in the initial consumeAvailable() call, which calls onFlagProcessed(), which
   * makes isAllPartitionsPaused() return false. The polling loop condition is checked but the
   * loop body never runs because the sentinel was already processed. This verifies the full
   * flow: sentinel → onFlagProcessed → partitions unpaused → no deadlock.
   */
  @Test
  public void testSentinelProcessedInCommitUnpausesPartitions() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    // Simulate the state after restart: all partitions paused, waiting for sentinel.
    // onFlagProcessed() will flip allPaused to false.
    boolean[] allPaused = {true};
    TableIdentifier[] flagProcessedTable = {null};
    CommittableSupplier committableSupplier = new CommittableSupplier() {
      @Override
      public Committable committable() {
        return new Committable(ImmutableMap.of(), ImmutableList.of());
      }

      @Override
      public Committable drainPendingFlagCommittable() {
        return null; // already drained in a previous commit() call
      }

      @Override
      public boolean isAllPartitionsPaused() {
        return allPaused[0];
      }

      @Override
      public void onFlagProcessed(TableIdentifier tableIdentifier) {
        flagProcessedTable[0] = tableIdentifier;
        allPaused[0] = false; // sentinel received — unpause
      }
    };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      // Add the per-table sentinel CommitToTable event to the control topic.
      consumer.addRecord(
          new ConsumerRecord<>(
              CONTROL_TOPIC_PARTITION.topic(),
              CONTROL_TOPIC_PARTITION.partition(),
              0,
              UUID.randomUUID().toString(),
              AvroUtil.encode(
                  new Event(
                      CONFIG.controlGroupId(),
                      new CommitToTable(
                          Coordinator.FLAG_PROCESSED_SENTINEL_ID,
                          TableReference.of(CATALOG_NAME, TABLE_1_IDENTIFIER),
                          0L,
                          null)))));

      // commit() must complete without deadlocking — the sentinel unblocks the loop
      committer.commit(committableSupplier);

      // onFlagProcessed must have been called with the correct table
      assertThat(flagProcessedTable[0])
          .as("onFlagProcessed() must be called when sentinel is received")
          .isEqualTo(TABLE_1_IDENTIFIER);

      // allPaused must now be false
      assertThat(allPaused[0])
          .as("Partitions must be unpaused after sentinel is processed")
          .isFalse();
    }
  }

  /**
   * Verifies that the polling loop in {@code commit()} runs and keeps polling when
   * {@code isAllPartitionsPaused()} returns true, and exits when it transitions to false.
   *
   * <p>This simulates the case where the sentinel arrives during the polling loop (not
   * during the initial consumeAvailable). The test uses a counter to simulate the sentinel
   * arriving after a few iterations.
   */
  @Test
  public void testPollingLoopRunsUntilPartitionsUnpaused() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    // Simulate isAllPartitionsPaused returning true for 3 iterations, then false
    int[] checkCount = {0};
    CommittableSupplier committableSupplier = new CommittableSupplier() {
      @Override
      public Committable committable() {
        return new Committable(ImmutableMap.of(), ImmutableList.of());
      }

      @Override
      public Committable drainPendingFlagCommittable() {
        return null;
      }

      @Override
      public boolean isAllPartitionsPaused() {
        checkCount[0]++;
        // Returns true for the first 3 checks, then false on the 4th
        return checkCount[0] <= 3;
      }
    };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      // commit() should run the loop 3 times, then exit on the 4th check
      committer.commit(committableSupplier);

      // isAllPartitionsPaused was checked: once entering the loop, then after each
      // iteration, for a total of 4 checks (3 true + 1 false)
      assertThat(checkCount[0])
          .as("Loop must run 3 iterations (3 true) and then exit (1 false)")
          .isEqualTo(4);
    }
  }

  /**
   * Verifies that the polling loop does NOT run when not all partitions are paused
   * (the normal, non-deadlock case).
   */
  @Test
  public void testPollingLoopDoesNotRunWhenNotAllPartitionsPaused() throws IOException {
    SinkTaskContext mockContext = mockContext();
    NoOpCoordinatorThreadFactory coordinatorThreadFactory = new NoOpCoordinatorThreadFactory();

    whenAdminListConsumerGroupOffsetsThenReturn(
        ImmutableMap.of(
            CONFIG.controlGroupId(), ImmutableMap.of(SOURCE_TP0, 110L, SOURCE_TP1, 100L)));

    int[] pauseCheckCount = {0};
    CommittableSupplier committableSupplier = new CommittableSupplier() {
      @Override
      public Committable committable() {
        throw new NotImplementedException("Should not be called");
      }

      @Override
      public Committable drainPendingFlagCommittable() {
        return null;
      }

      @Override
      public boolean isAllPartitionsPaused() {
        pauseCheckCount[0]++;
        return false; // not all paused — loop should not run
      }
    };

    try (CommitterImpl committerImpl =
        new CommitterImpl(mockContext, CONFIG, kafkaClientFactory, coordinatorThreadFactory)) {
      initConsumer();
      Committer committer = committerImpl;

      committer.commit(committableSupplier);

      // isAllPartitionsPaused should have been checked exactly once (the while condition)
      assertThat(pauseCheckCount[0])
          .as("isAllPartitionsPaused() should be checked once and return false")
          .isEqualTo(1);
    }
  }
}
