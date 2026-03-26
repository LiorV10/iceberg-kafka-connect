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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.FlagWriterResult;
import io.tabular.iceberg.connect.data.IcebergWriter;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.WriterResult;
import io.tabular.iceberg.connect.events.EventTestUtil;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

public class WorkerTest {
  private static final String SRC_TOPIC_NAME = "src-topic";
  private static final String TABLE_NAME = "db.tbl";
  private static final String FIELD_NAME = "fld";
  private static final String FLAG_PREFIX = "__flag__";

  @Test
  public void testStaticRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.tables()).thenReturn(ImmutableList.of(TABLE_NAME));
    when(config.catalogName()).thenReturn("catalog");
    when(config.flagKeyPrefix()).thenReturn(null); // no flag detection
    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, "val");
    workerTest(config, value);
  }

  @Test
  public void testDynamicRoute() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.catalogName()).thenReturn("catalog");
    when(config.flagKeyPrefix()).thenReturn(null); // no flag detection

    Map<String, Object> value = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    workerTest(config, value);
  }

  /**
   * A flag record seen in write() is immediately included in the next committable() —
   * the Worker handles exactly one partition so no cross-partition barrier is needed.
   */
  @Test
  public void testFlagIsImmediatelyIncludedInCommittable() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    Worker worker = new Worker(config, writerFactory);

    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));

    // Flag must be present immediately in the committable after the write
    assertThat(worker.committable().writerResults())
        .as("Flag must be immediately active in the committable after write()")
        .hasSize(1)
        .allMatch(r -> r instanceof FlagWriterResult);
  }

  /**
   * Verifies that the serialized flag JSON includes key, topic, partition, offset,
   * timestamp, and value — not just the record value — when the value is a Map.
   */
  @Test
  public void testFlagJsonIncludesRecordMetadata() throws Exception {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    Worker worker = new Worker(config, writerFactory);

    String flagKey = FLAG_PREFIX + "end";
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec = new SinkRecord(SRC_TOPIC_NAME, 3, null, flagKey, null, flagValue, 42L);
    worker.write(ImmutableList.of(flagRec));

    WriterResult result = worker.committable().writerResults().get(0);
    assertThat(result).isInstanceOf(FlagWriterResult.class);

    // The data file path is "__flag__" + recordJson
    String path = result.dataFiles().get(0).path().toString();
    assertThat(path).startsWith(FlagWriterResult.FLAG_PREFIX);

    String json = path.substring(FlagWriterResult.FLAG_PREFIX.length());
    JsonNode node = new ObjectMapper().readTree(json);

    assertThat(node.get("topic").asText()).isEqualTo(SRC_TOPIC_NAME);
    assertThat(node.get("partition").asInt()).isEqualTo(3);
    assertThat(node.get("offset").asLong()).isEqualTo(42L);
    assertThat(node.get("key").asText()).isEqualTo(flagKey);
    assertThat(node.has("timestamp")).isTrue();
    // value is a Map — its fields should be present under "value"
    assertThat(node.get("value").get(FIELD_NAME).asText()).isEqualTo(TABLE_NAME);
  }


  @Test
  public void testWorkerPausesWhenFlagDetected() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    TopicPartition tp = new TopicPartition(SRC_TOPIC_NAME, 0);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    Worker worker = new Worker(config, writerFactory, context);

    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));

    // pause() must be called IMMEDIATELY upon detecting the flag in write() — not deferred to
    // committable().  This is necessary because if all source partitions are paused, Kafka
    // Connect stops calling put(), and the flag must already be in flagWriterResults before
    // the control topic is polled via preCommit().
    verify(context, times(1)).pause(tp);
    // resume() must NOT have been called yet
    verify(context, never()).resume(tp);
  }

  /**
   * Verifies that records arriving after the flag in the same batch are written to their natural
   * destination (no rerouting).  The partition is paused immediately on flag detection so no
   * further batches arrive, but same-batch records go where their route field directs them.
   */
  @Test
  public void testPostFlagRecordsInSameBatchGoToNaturalTable() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    TopicPartition tp = new TopicPartition(SRC_TOPIC_NAME, 0);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));

    IcebergWriter dataWriter = mock(IcebergWriter.class);
    when(dataWriter.complete()).thenReturn(ImmutableList.of());
    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(dataWriter);

    Worker worker = new Worker(config, writerFactory, context);

    // A data record that naturally routes to "db.other", arriving after the flag in the same batch
    String otherTable = "db.other";
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    SinkRecord dataRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null,
            ImmutableMap.of(FIELD_NAME, otherTable), 2L);

    worker.write(ImmutableList.of(flagRec, dataRec));

    // Post-flag record must be written to its NATURAL table — no rerouting to the flag table
    verify(writerFactory, times(1)).createWriter(eq(otherTable), any(), anyBoolean());
    verify(writerFactory, never()).createWriter(eq(TABLE_NAME), any(), anyBoolean());
  }

  /**
   * Verifies that the flag record's offset is NOT included in the committable before the flag
   * has been processed.  The offset should remain pending until {@link Worker#onFlagProcessed}
   * is called so that a pod crash before processing causes the flag to be re-read on restart.
   */
  @Test
  public void testFlagOffsetNotCommittedBeforeOnFlagProcessed() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    Worker worker = new Worker(config, writerFactory);

    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 5L);
    worker.write(ImmutableList.of(flagRec));

    // The committable must NOT contain an offset for the flag partition —
    // the flag offset must be deferred until onFlagProcessed().
    Committable committable = worker.committable();
    assertThat(committable.offsetsByTopicPartition())
        .as("Flag offset must not appear in committable before the flag is processed")
        .isEmpty();
  }

  /**
   * Verifies that post-flag records (in the same batch as the flag) do NOT advance
   * {@code sourceOffsets} past the flag position.  Their offsets must be deferred to
   * {@code pendingFlagOffsets} so that a pod crash (before {@link Worker#onFlagProcessed}
   * is called) rewinds the consumer to <em>before</em> the flag, not past it.
   *
   * <p>This is the core crash-recovery guarantee: even if the flag plus post-flag records
   * arrive together, the committed offset must stay at the last pre-flag record so the flag
   * is always re-read on restart.
   */
  @Test
  public void testPostFlagRecordsOffsetsDeferred() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    IcebergWriter dataWriter = mock(IcebergWriter.class);
    when(dataWriter.complete()).thenReturn(ImmutableList.of());
    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(dataWriter);

    TopicPartition tp = new TopicPartition(SRC_TOPIC_NAME, 0);
    Worker worker = new Worker(config, writerFactory);

    // Pre-flag record at offset 99 → sourceOffsets[P0] = 100
    SinkRecord preRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key-pre", null,
            ImmutableMap.of(FIELD_NAME, TABLE_NAME), 99L);
    // Flag record at offset 100
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null,
            ImmutableMap.of(FIELD_NAME, TABLE_NAME), 100L);
    // Post-flag records at offsets 101 and 102 — these must NOT advance sourceOffsets past 100
    SinkRecord postRec1 =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key-post1", null,
            ImmutableMap.of(FIELD_NAME, TABLE_NAME), 101L);
    SinkRecord postRec2 =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key-post2", null,
            ImmutableMap.of(FIELD_NAME, TABLE_NAME), 102L);

    worker.write(ImmutableList.of(preRec, flagRec, postRec1, postRec2));

    // The committable's sourceOffsets must reflect ONLY the pre-flag record (offset 100),
    // not the post-flag records (101 or 102).  This means on crash the consumer is rewound
    // to offset 100 (the flag itself), so the flag is re-read.
    Committable committable = worker.committable();
    assertThat(committable.offsetsByTopicPartition())
        .as("Committable offsets must only contain the pre-flag offset")
        .containsKey(tp);
    assertThat(committable.offsetsByTopicPartition().get(tp).offset())
        .as("Offset must be 100 (one past pre-flag record at 99), not past the flag")
        .isEqualTo(100L);
  }

  /**
   * Simulates crash recovery when a flag was in a batch together with post-flag records.
   * After the simulated crash (fresh Worker), the flag is re-read and a subsequent
   * {@link Worker#committable()} call must include the flag result — verifying that
   * "committable is never called (with flag data)" is fixed.
   */
  @Test
  public void testCrashRecoveryFlagRereadWithPostFlagRecords() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    IcebergWriter dataWriter = mock(IcebergWriter.class);
    when(dataWriter.complete()).thenReturn(ImmutableList.of());
    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(dataWriter);

    // --- Run 1: flag + post-flag records arrive, pod crashes before onFlagProcessed ---
    Worker worker1 = new Worker(config, writerFactory);

    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null,
            ImmutableMap.of(FIELD_NAME, TABLE_NAME), 100L);
    SinkRecord postRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key-post", null,
            ImmutableMap.of(FIELD_NAME, TABLE_NAME), 101L);

    worker1.write(ImmutableList.of(flagRec, postRec));

    // The committed offset after committable() is the PRE-flag sourceOffsets value (empty here
    // since no pre-flag records), so on crash the consumer rewinds to before offset 100.
    Committable run1Committable = worker1.committable();
    // sourceOffsets is empty (no pre-flag records in this run), so committed offset for P0 is null.
    // This means the consumer group will stay at its previous committed value (before offset 100),
    // and the flag at 100 will be re-read on restart.
    assertThat(run1Committable.offsetsByTopicPartition())
        .as("No pre-flag sourceOffset: committed map must be empty (flag offset deferred)")
        .isEmpty();

    // --- Run 2 (after crash): flag is re-read from offset 100 ---
    Worker worker2 = new Worker(config, writerFactory);

    // Re-read the same flag record (as crash recovery would deliver it again)
    worker2.write(ImmutableList.of(flagRec));

    // committable() MUST now include the flag result — this is what was broken before the fix
    Committable run2Committable = worker2.committable();
    assertThat(run2Committable.writerResults())
        .as("Flag result must be present in committable after crash recovery re-read")
        .hasSize(1)
        .allMatch(r -> r instanceof io.tabular.iceberg.connect.data.FlagWriterResult);
  }

  /**
   * Verifies that after {@link Worker#onFlagProcessed} is called for the matching table,
   * the flag record's offset is moved to sourceOffsets and appears in the next committable.
   * This ensures the offset is committed to the control group only after the Coordinator
   * has executed the flag action.
   */
  @Test
  public void testFlagOffsetAppearsInCommittableAfterOnFlagProcessed() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    TopicPartition tp = new TopicPartition(SRC_TOPIC_NAME, 0);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    Worker worker = new Worker(config, writerFactory, context);

    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 5L);
    worker.write(ImmutableList.of(flagRec));

    // First committable: flag offset must be absent
    worker.committable();

    // Coordinator signals flag processed — pending offset moves to sourceOffsets
    worker.onFlagProcessed(TableIdentifier.parse(TABLE_NAME));

    // Next committable must contain the flag partition's offset
    Committable nextCommittable = worker.committable();
    assertThat(nextCommittable.offsetsByTopicPartition())
        .as("Flag offset must appear in committable after onFlagProcessed()")
        .containsKey(tp);
    assertThat(nextCommittable.offsetsByTopicPartition().get(tp).offset())
        .as("Offset must be one past the flag record offset")
        .isEqualTo(6L);
  }

  /**
   * Verifies that {@link Worker#onFlagProcessed} is a no-op for a worker that never detected
   * a flag, regardless of which table is passed.
   */
  @Test
  public void testOnFlagProcessedIsNoOpWhenNoFlagSeen() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    TopicPartition tp = new TopicPartition(SRC_TOPIC_NAME, 0);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    Worker worker = new Worker(config, writerFactory, context);

    // No flag was ever written to this worker — simulate a spurious/erroneous call anyway
    worker.onFlagProcessed(TableIdentifier.parse(TABLE_NAME));

    // Neither pause nor resume should have been called
    verify(context, never()).pause(tp);
    verify(context, never()).resume(tp);
  }

  /**
   * Verifies that {@link Worker#onFlagProcessed} resumes the paused partitions and clears the
   * pending flag state when called with the matching table identifier.
   */
  @Test
  public void testPendingFlagClearedAndPartitionsResumedOnFlagProcessed() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    TopicPartition tp = new TopicPartition(SRC_TOPIC_NAME, 0);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));

    IcebergWriter dataWriter = mock(IcebergWriter.class);
    when(dataWriter.complete()).thenReturn(ImmutableList.of());
    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(dataWriter);

    Worker worker = new Worker(config, writerFactory, context);

    // Write a flag record for TABLE_NAME — partition must be paused immediately
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));

    // Signal that the Coordinator finished the branch switch for TABLE_NAME
    worker.onFlagProcessed(TableIdentifier.parse(TABLE_NAME));

    // Partitions must be resumed
    verify(context, times(1)).resume(tp);

    // After resume, records route to their natural destination (pendingFlagTable cleared)
    String naturalTable = "db.natural";
    SinkRecord dataRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null,
            ImmutableMap.of(FIELD_NAME, naturalTable), 2L);
    worker.write(ImmutableList.of(dataRec));

    verify(writerFactory, times(1)).createWriter(eq(naturalTable), any(), anyBoolean());
    verify(writerFactory, never()).createWriter(eq(TABLE_NAME), any(), anyBoolean());
  }

  /**
   * Verifies that {@link Worker#onFlagProcessed} is a no-op when called with a different table
   * identifier than the one the worker has a pending flag for.  This ensures a per-table sentinel
   * for another table does not accidentally resume a worker that is waiting for its own table.
   */
  @Test
  public void testOnFlagProcessedIsNoOpForDifferentTable() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    TopicPartition tp = new TopicPartition(SRC_TOPIC_NAME, 0);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));

    IcebergWriter dataWriter = mock(IcebergWriter.class);
    when(dataWriter.complete()).thenReturn(ImmutableList.of());
    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(dataWriter);

    Worker worker = new Worker(config, writerFactory, context);

    // Write a flag for TABLE_NAME — this puts the worker in "pending flag" state
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));

    // Signal flag processed for a DIFFERENT table — this worker must stay paused
    worker.onFlagProcessed(TableIdentifier.parse("db.other_table"));

    // Resume must NOT have been called — this worker is still waiting for its own table's flag
    verify(context, never()).resume(tp);

    // The pending flag state is still active: a subsequent record on the same partition
    // still has its offset deferred to pendingFlagOffsets (crash safety still applies)
    SinkRecord postRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null,
            ImmutableMap.of(FIELD_NAME, "db.natural"), 2L);
    worker.write(ImmutableList.of(postRec));
    // Record is written to its natural table (no rerouting)
    verify(writerFactory, times(1)).createWriter(eq("db.natural"), any(), anyBoolean());
    verify(writerFactory, never()).createWriter(eq(TABLE_NAME), any(), anyBoolean());
    // And the offset for this partition is still deferred — committable() must NOT include it
    assertThat(worker.committable().offsetsByTopicPartition())
        .as("Offset must remain deferred until onFlagProcessed() fires for the correct table")
        .doesNotContainKey(tp);
  }

  /**
   * Verifies that pre-flag records in the same batch are written immediately to their natural
   * destination, and that post-flag records also go to their natural destination (no rerouting).
   */
  @Test
  public void testPreFlagAndPostFlagRecordsWrittenToNaturalDestination() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    TopicPartition tp = new TopicPartition(SRC_TOPIC_NAME, 0);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp));

    IcebergWriter dataWriter = mock(IcebergWriter.class);
    when(dataWriter.complete()).thenReturn(ImmutableList.of());
    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(dataWriter);

    Worker worker = new Worker(config, writerFactory, context);

    String preTable = "db.pre";
    String postTable = "db.post";
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord preRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key-pre", null,
            ImmutableMap.of(FIELD_NAME, preTable), 1L);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 2L);
    SinkRecord postRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key-post", null,
            ImmutableMap.of(FIELD_NAME, postTable), 3L);

    worker.write(ImmutableList.of(preRec, flagRec, postRec));

    // Pre-flag record is written to its natural table
    verify(writerFactory, times(1)).createWriter(eq(preTable), any(), anyBoolean());
    // Post-flag record is also written to its natural table — no rerouting
    verify(writerFactory, times(1)).createWriter(eq(postTable), any(), anyBoolean());
    // The flag's table (TABLE_NAME) is never used as a writer destination for data records
    verify(writerFactory, never()).createWriter(eq(TABLE_NAME), any(), anyBoolean());
  }

  private void workerTest(IcebergSinkConfig config, Map<String, Object> value) {
    WriterResult writeResult =
        new WriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(writeResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Writer worker = new Worker(config, writerFactory);

    // save a record
    SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
    worker.write(ImmutableList.of(rec));

    Committable committable = worker.committable();

    assertThat(committable.offsetsByTopicPartition()).hasSize(1);
    // offset should be one more than the record offset
    assertThat(
            committable
                .offsetsByTopicPartition()
                .get(committable.offsetsByTopicPartition().keySet().iterator().next())
                .offset())
        .isEqualTo(1L);
  }
}
