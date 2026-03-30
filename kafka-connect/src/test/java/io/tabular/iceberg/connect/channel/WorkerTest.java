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

    // pause() is called immediately when the flag is detected during write()
    verify(context, times(1)).pause(tp);
  }

  /**
   * Verifies that records arriving after the flag in the same batch are dropped (not written)
   * because the worker enters paused state upon detecting the flag.  These records will be
   * re-delivered by Kafka Connect after the flag is processed and the partition is resumed.
   */
  @Test
  public void testPostFlagRecordsInSameBatchAreDroppedWhenPaused() {
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

    // A data record whose natural route is "db.other", but arrives after the flag in the batch
    String otherTable = "db.other";
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    SinkRecord dataRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null,
            ImmutableMap.of(FIELD_NAME, otherTable), 2L);

    worker.write(ImmutableList.of(flagRec, dataRec));

    // Post-flag non-flag record is dropped (paused), no writer is created for it
    verify(writerFactory, never()).createWriter(eq(otherTable), any(), anyBoolean());
    // The flag itself does not create a data writer either (it creates a FlagWriterResult)
    verify(writerFactory, never()).createWriter(eq(TABLE_NAME), any(), anyBoolean());
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
   * reroute when called with the matching table identifier.
   */
  @Test
  public void testRerouteIsClearedAndPartitionsResumedOnFlagProcessed() {
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

    // Activate pause by writing a flag record for TABLE_NAME
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));

    // Signal that the Coordinator finished the branch switch for TABLE_NAME
    worker.onFlagProcessed(TableIdentifier.parse(TABLE_NAME));

    // Partitions must be resumed (resume is also called inside pauseAssignment as a reset)
    verify(context, times(2)).resume(tp);

    // After resume, records must go to their natural destination (not the flag branch)
    String naturalTable = "db.natural";
    SinkRecord dataRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null,
            ImmutableMap.of(FIELD_NAME, naturalTable), 2L);
    worker.write(ImmutableList.of(dataRec));

    verify(writerFactory, times(1)).createWriter(eq(naturalTable), any(), anyBoolean());
    verify(writerFactory, never()).createWriter(eq(TABLE_NAME), any(), anyBoolean());
  }

  /**
   * Verifies that {@link Worker#onFlagProcessed} resumes the worker even when called with a
   * different table identifier.  The Worker's pause is a global state for a single flag broadcast
   * event; filtering by table is the Coordinator's responsibility.
   */
  @Test
  public void testOnFlagProcessedResumesRegardlessOfTableIdentifier() {
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

    // Activate pause by writing a flag for TABLE_NAME
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));

    // Signal flag processed — even though the table identifier differs, the worker resumes
    worker.onFlagProcessed(TableIdentifier.parse("db.other_table"));

    // Resume should have been called by resumeAssignment
    // (pauseAssignment also calls resume as a reset before pausing)
    TopicPartition[] expectedPartitions = new TopicPartition[]{tp};
    verify(context, times(2)).resume(expectedPartitions);
  }

  /**
   * Verifies that pre-flag records in the same batch are written immediately to their natural
   * destination, while post-flag non-flag records are dropped because the worker is paused.
   */
  @Test
  public void testPreFlagRecordsWrittenNormallyPostFlagRecordsDropped() {
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
    // Post-flag non-flag record is dropped (paused) — not rerouted, not written
    verify(writerFactory, never()).createWriter(eq(postTable), any(), anyBoolean());
    verify(writerFactory, never()).createWriter(eq(TABLE_NAME), any(), anyBoolean());
  }

  /**
   * Verifies that when flags from multiple partitions arrive in the same batch (e.g. flags
   * broadcast to all partitions before the connector starts), ALL flags are processed —
   * not just the first one.  This ensures all partition votes reach the Coordinator for quorum.
   */
  @Test
  public void testMultiPartitionFlagsInSameBatchAllProcessed() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    TopicPartition tp0 = new TopicPartition(SRC_TOPIC_NAME, 0);
    TopicPartition tp1 = new TopicPartition(SRC_TOPIC_NAME, 1);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp0, tp1));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    Worker worker = new Worker(config, writerFactory, context);

    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagP0 =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 10L);
    SinkRecord flagP1 =
        new SinkRecord(SRC_TOPIC_NAME, 1, null, FLAG_PREFIX + "end", null, flagValue, 20L);

    // Both flags arrive in the same batch
    worker.write(ImmutableList.of(flagP0, flagP1));

    Committable committable = worker.committable();

    // Both flags must be included in the committable as FlagWriterResults
    assertThat(committable.writerResults())
        .hasSize(2)
        .allMatch(r -> r instanceof FlagWriterResult);

    // Both partitions' offsets must be tracked
    assertThat(committable.offsetsByTopicPartition()).hasSize(2);
    assertThat(committable.offsetsByTopicPartition().get(tp0).offset()).isEqualTo(10L);
    assertThat(committable.offsetsByTopicPartition().get(tp1).offset()).isEqualTo(20L);
  }

  /**
   * Verifies that when flags from multiple partitions are processed and onFlagProcessed
   * is called, ALL paused partitions are resumed and offsets are advanced past all flags.
   */
  @Test
  public void testMultiPartitionOnFlagProcessedAdvancesAllOffsets() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    TopicPartition tp0 = new TopicPartition(SRC_TOPIC_NAME, 0);
    TopicPartition tp1 = new TopicPartition(SRC_TOPIC_NAME, 1);
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.of(tp0, tp1));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    Worker worker = new Worker(config, writerFactory, context);

    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagP0 =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 10L);
    SinkRecord flagP1 =
        new SinkRecord(SRC_TOPIC_NAME, 1, null, FLAG_PREFIX + "end", null, flagValue, 20L);

    worker.write(ImmutableList.of(flagP0, flagP1));
    // Consume the first committable (which sends the flag data to the Coordinator)
    worker.committable();

    // Now simulate the Coordinator processing all flag votes and sending the sentinel
    worker.onFlagProcessed(TableIdentifier.parse(TABLE_NAME));

    // context.offset should be called for both partitions to advance past their flags
    verify(context).offset(tp0, 11L);
    verify(context).offset(tp1, 21L);

    // After resume, new records from either partition are processed normally
    IcebergWriter dataWriter = mock(IcebergWriter.class);
    when(dataWriter.complete()).thenReturn(ImmutableList.of());
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(dataWriter);

    String naturalTable = "db.natural";
    SinkRecord dataRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null,
            ImmutableMap.of(FIELD_NAME, naturalTable), 11L);
    worker.write(ImmutableList.of(dataRec));

    verify(writerFactory, times(1)).createWriter(eq(naturalTable), any(), anyBoolean());
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
