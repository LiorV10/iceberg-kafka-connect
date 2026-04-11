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
import static org.mockito.Mockito.atLeast;
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

    // pause() is called immediately during write() when a flag is detected
    verify(context, times(1)).pause(new TopicPartition[]{tp});
  }

  /**
   * Verifies that records arriving after the flag in the same batch are skipped (not written)
   * because the worker enters the paused state. They will be re-delivered after the coordinator
   * processes the flag and the partition is resumed.
   */
  @Test
  public void testPostFlagRecordsInSameBatchAreSkipped() {
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

    String otherTable = "db.other";
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    SinkRecord dataRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null,
            ImmutableMap.of(FIELD_NAME, otherTable), 2L);

    worker.write(ImmutableList.of(flagRec, dataRec));

    // Post-flag data record must be skipped — no writer created for any data table
    verify(writerFactory, never()).createWriter(any(), any(), anyBoolean());
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
   * Verifies that {@link Worker#onFlagProcessed} resumes the paused partitions so that
   * new records are processed normally after the coordinator handles the flag.
   */
  @Test
  public void testPartitionsResumedOnFlagProcessed() {
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

    // Partitions must be resumed by onFlagProcessed. Note: pauseAssignment() also
    // calls resume() internally before pause(), so we use atLeast(2):
    // once from pauseAssignment and once from resumeAssignment.
    verify(context, atLeast(2)).resume(any());

    // After resume, records must go to their natural destination
    String naturalTable = "db.natural";
    SinkRecord dataRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null,
            ImmutableMap.of(FIELD_NAME, naturalTable), 2L);
    worker.write(ImmutableList.of(dataRec));

    verify(writerFactory, times(1)).createWriter(eq(naturalTable), any(), anyBoolean());
  }

  /**
   * Verifies that {@link Worker#onFlagProcessed} resumes the worker regardless of which table
   * identifier is passed, as long as the worker is currently paused.  The current pause/resume
   * implementation does not distinguish tables — the sentinel signal always triggers resume.
   */
  @Test
  public void testOnFlagProcessedResumesRegardlessOfTable() {
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

    // Signal flag processed for a DIFFERENT table — worker still resumes because
    // the current implementation does not filter by table identifier
    worker.onFlagProcessed(TableIdentifier.parse("db.other_table"));

    // Resume should be called by onFlagProcessed. Note: pauseAssignment() also
    // calls resume() internally before pause(), so we use atLeast(2).
    verify(context, atLeast(2)).resume(any());
  }

  /**
   * Verifies that pre-flag records in the same batch are written normally to their natural
   * destination, while post-flag non-flag records are skipped (paused).
   */
  @Test
  public void testPreFlagRecordsWrittenNormallyPostFlagRecordsSkipped() {
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
    // Post-flag data record is skipped — worker is paused
    verify(writerFactory, never()).createWriter(eq(postTable), any(), anyBoolean());
  }

  /**
   * Verifies that when multiple flag records arrive in the same batch, ALL of them are
   * processed and included in the committable. This is the core fix for the issue where
   * only the first flag was processed per pause/resume cycle.
   */
  @Test
  public void testMultipleFlagsInSameBatchAreAllProcessed() {
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

    Map<String, Object> flagValue1 = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    Map<String, Object> flagValue2 = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    Map<String, Object> flagValue3 = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flag1 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "type1", null, flagValue1, 10L);
    SinkRecord flag2 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "type2", null, flagValue2, 11L);
    SinkRecord flag3 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "type3", null, flagValue3, 12L);

    worker.write(ImmutableList.of(flag1, flag2, flag3));

    // All 3 flags must be in the committable
    Committable committable = worker.committable();
    assertThat(committable.writerResults())
        .as("All consecutive flags in the batch must be processed")
        .hasSize(3)
        .allMatch(r -> r instanceof FlagWriterResult);
  }

  /**
   * Verifies that when multiple flags arrive in a batch with data records interspersed,
   * the flags are all processed, data records before the first flag are written normally,
   * and data records after the first flag are skipped.
   */
  @Test
  public void testMultipleFlagsWithInterleavedDataRecords() {
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
    String midTable = "db.mid";
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord dataRec1 = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key1", null,
        ImmutableMap.of(FIELD_NAME, preTable), 1L);
    SinkRecord flag1 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "type1", null, flagValue, 2L);
    SinkRecord dataRec2 = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key2", null,
        ImmutableMap.of(FIELD_NAME, midTable), 3L);
    SinkRecord flag2 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "type2", null, flagValue, 4L);

    worker.write(ImmutableList.of(dataRec1, flag1, dataRec2, flag2));

    // Pre-flag data record is written normally
    verify(writerFactory, times(1)).createWriter(eq(preTable), any(), anyBoolean());
    // Data record between flags is skipped (worker is paused)
    verify(writerFactory, never()).createWriter(eq(midTable), any(), anyBoolean());

    // Both flags are included in the committable
    Committable committable = worker.committable();
    long flagCount = committable.writerResults().stream()
        .filter(r -> r instanceof FlagWriterResult)
        .count();
    assertThat(flagCount)
        .as("Both flags must be included in committable")
        .isEqualTo(2);
  }

  /**
   * Verifies that after processing multiple flags, onFlagProcessed resumes from
   * the last flag's offset + 1.
   */
  @Test
  public void testOnFlagProcessedResumesFromLastFlagOffset() {
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
    SinkRecord flag1 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "type1", null, flagValue, 10L);
    SinkRecord flag2 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "type2", null, flagValue, 11L);
    SinkRecord flag3 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "type3", null, flagValue, 12L);

    worker.write(ImmutableList.of(flag1, flag2, flag3));

    // Consume the committable to clear internal state
    worker.committable();

    // Signal flag processed
    worker.onFlagProcessed(TableIdentifier.parse(TABLE_NAME));

    // context.offset must be called with last flag offset + 1 = 13
    verify(context).offset(tp, 13L);
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
