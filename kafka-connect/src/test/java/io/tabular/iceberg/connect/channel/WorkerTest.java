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

    // pause() must be called immediately when the flag is detected
    verify(context, times(1)).pause(tp);
    // resume() must NOT have been called yet
    verify(context, never()).resume(tp);
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
   * Verifies that {@link Worker#onFlagProcessed} resumes the paused partitions when called
   * with the matching table identifier, and that subsequent records are routed normally.
   */
  @Test
  public void testPartitionsAreResumedOnFlagProcessed() {
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

    // Pause the worker by writing a flag record for TABLE_NAME
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));

    // Signal that the Coordinator finished the branch switch for TABLE_NAME
    worker.onFlagProcessed(TableIdentifier.parse(TABLE_NAME));

    // Partitions must be resumed
    verify(context, times(1)).resume(tp);

    // After resume, records must go to their natural destination
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
   * identifier than the one the worker is currently paused for.  This ensures a per-table sentinel
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

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);

    Worker worker = new Worker(config, writerFactory, context);

    // Pause by writing a flag for TABLE_NAME
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));

    // Signal flag processed for a DIFFERENT table — this worker must NOT resume
    worker.onFlagProcessed(TableIdentifier.parse("db.other_table"));

    // Resume must NOT have been called — still paused waiting for its own table's sentinel
    verify(context, never()).resume(tp);
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
