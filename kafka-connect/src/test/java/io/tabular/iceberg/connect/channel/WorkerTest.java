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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
   * Verifies that when a flag record is encountered, the worker immediately pauses all assigned
   * source-topic partitions via {@link SinkTaskContext#pause}.
   */
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

    // pause() must have been called exactly once with the assigned partition
    verify(context, times(1)).pause(tp);
    // resume() must NOT have been called yet
    verify(context, times(0)).resume(tp);
  }

  /**
   * Verifies that records arriving after the flag in the same batch are buffered and only written
   * once {@link Worker#onFlagProcessed()} is called (which also resumes the partitions).
   */
  @Test
  public void testPostFlagRecordsAreBufferedAndProcessedOnFlagProcessed() {
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

    String dataTable = "db.after_flag";
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    Map<String, Object> dataValue = ImmutableMap.of(FIELD_NAME, dataTable);
    SinkRecord flagRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    SinkRecord dataRec =
        new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, dataValue, 2L);

    // Write flag followed immediately by a data record in the same batch
    worker.write(ImmutableList.of(flagRec, dataRec));

    // The data record that came after the flag must NOT have been written yet
    verify(writerFactory, times(0)).createWriter(eq(dataTable), any(), anyBoolean());

    // Signal that the Coordinator finished the branch switch
    worker.onFlagProcessed();

    // Partitions must be resumed with the assigned partition
    verify(context, times(1)).resume(tp);

    // The buffered data record must now have been written to its natural destination
    verify(writerFactory, times(1)).createWriter(eq(dataTable), any(), anyBoolean());
  }

  /**
   * Verifies that pre-flag records in the same batch as the flag are written immediately and their
   * offsets are committed, while post-flag records wait for {@link Worker#onFlagProcessed()}.
   */
  @Test
  public void testPreFlagRecordsWrittenImmediatelyPostFlagRecordsBuffered() {
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

    // Pre-flag record must be written immediately
    verify(writerFactory, times(1)).createWriter(eq(preTable), any(), anyBoolean());
    // Post-flag record must NOT be written yet
    verify(writerFactory, times(0)).createWriter(eq(postTable), any(), anyBoolean());

    // After flag processed, post-flag record is written
    worker.onFlagProcessed();
    verify(writerFactory, times(1)).createWriter(eq(postTable), any(), anyBoolean());
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
