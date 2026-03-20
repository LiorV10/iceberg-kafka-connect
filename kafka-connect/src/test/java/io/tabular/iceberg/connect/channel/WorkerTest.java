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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.FlagWriterResult;
import io.tabular.iceberg.connect.data.IcebergWriter;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.WriterResult;
import io.tabular.iceberg.connect.events.EventTestUtil;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.connect.sink.SinkRecord;
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
   * Validates the fix for the "N-flags-from-one-partition" bug.
   *
   * <p>Message queue: [row, end-flag, row, end-flag].
   * Before the fix the two data rows ended up merged in a single writer and the two flag
   * events were deduplicated into one, producing [row+row, flag].
   * After the fix the worker segments its output at each flag boundary, producing:
   *   [data1, flag1, data2, flag2]
   * — i.e. each data segment is followed immediately by its own flag.
   */
  @Test
  public void testRowFlagRowFlag_producesOrderedSegments() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.flagTypeField()).thenReturn("type");
    when(config.branchesRegexDelimiter()).thenReturn(null);

    // Each write() call on the mocked writer completes immediately (no files produced,
    // but complete() returns a distinct WriterResult so we can track segment identity).
    WriterResult segmentResult1 = new WriterResult(
        TableIdentifier.parse(TABLE_NAME), ImmutableList.of(), ImmutableList.of(), StructType.of());
    WriterResult segmentResult2 = new WriterResult(
        TableIdentifier.parse(TABLE_NAME), ImmutableList.of(), ImmutableList.of(), StructType.of());

    IcebergWriter writer1 = mock(IcebergWriter.class);
    IcebergWriter writer2 = mock(IcebergWriter.class);
    when(writer1.complete()).thenReturn(ImmutableList.of(segmentResult1));
    when(writer2.complete()).thenReturn(ImmutableList.of(segmentResult2));

    // writerFactory returns writer1 the first time and writer2 the second time
    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean()))
        .thenReturn(writer1)   // for the first "row" (before flag1)
        .thenReturn(writer2);  // for the second "row" (after flag1, before flag2)

    Worker worker = new Worker(config, writerFactory);

    Map<String, Object> rowValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME, "type", "END-LOAD");

    SinkRecord row1 = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key",  null, rowValue,  0L);
    SinkRecord flag1 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    SinkRecord row2 = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key",  null, rowValue,  2L);
    SinkRecord flag2 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 3L);

    worker.write(ImmutableList.of(row1, flag1, row2, flag2));

    List<WriterResult> results = worker.committable().writerResults();

    // Expected ordering: [data1, flag1, data2, flag2]
    assertThat(results).hasSize(4);
    assertThat(results.get(0)).isSameAs(segmentResult1);
    assertThat(results.get(1)).isInstanceOf(FlagWriterResult.class);
    assertThat(results.get(2)).isSameAs(segmentResult2);
    assertThat(results.get(3)).isInstanceOf(FlagWriterResult.class);

    // The two flags must carry different seqnos (1 and 2) so the Coordinator can vote on them
    // independently.
    FlagWriterResult fwr1 = (FlagWriterResult) results.get(1);
    FlagWriterResult fwr2 = (FlagWriterResult) results.get(3);
    String path1 = fwr1.dataFiles().get(0).path().toString()
        .substring(FlagWriterResult.FLAG_PREFIX.length());
    String path2 = fwr2.dataFiles().get(0).path().toString()
        .substring(FlagWriterResult.FLAG_PREFIX.length());
    assertThat(path1).contains("\"" + Worker.FLAG_SEQNO_FIELD + "\":1");
    assertThat(path2).contains("\"" + Worker.FLAG_SEQNO_FIELD + "\":2");
  }

  /**
   * Validates multi-partition broadcast flag handling within a single task.
   *
   * <p>When a single task owns multiple partitions (e.g. partition 0 and 1), the producer
   * broadcasts each flag to ALL partitions.  The task therefore sees two copies of every
   * logical flag: one from partition 0 and one from partition 1.
   *
   * <p>The seqno must be tracked PER SOURCE PARTITION so that both broadcast copies of the
   * same logical flag get the same seqno (1 for the first occurrence, 2 for the second …).
   * Without per-partition tracking the first copy gets seqno=1 and the second copy gets
   * seqno=2 — the Coordinator would accumulate votes in two separate buckets and never
   * reach quorum for either.
   *
   * <p>Message stream (interleaved from 2 partitions):
   * [row_p0, row_p1, flag_p0(END-LOAD), flag_p1(END-LOAD), row_p0, row_p1, flag_p0(END-LOAD), flag_p1(END-LOAD)]
   *
   * <p>Expected output ordering (Writer segments):
   * [data{row_p0+row_p1}, flag_p0(seqno=1), flag_p1(seqno=1), data{row_p0+row_p1}, flag_p0(seqno=2), flag_p1(seqno=2)]
   */
  @Test
  public void testMultiPartitionBroadcastFlags_sameSeqno() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.flagTypeField()).thenReturn("type");
    when(config.branchesRegexDelimiter()).thenReturn(null);

    WriterResult segmentResult1 = new WriterResult(
        TableIdentifier.parse(TABLE_NAME), ImmutableList.of(), ImmutableList.of(), StructType.of());
    WriterResult segmentResult2 = new WriterResult(
        TableIdentifier.parse(TABLE_NAME), ImmutableList.of(), ImmutableList.of(), StructType.of());

    IcebergWriter writer1 = mock(IcebergWriter.class);
    IcebergWriter writer2 = mock(IcebergWriter.class);
    when(writer1.complete()).thenReturn(ImmutableList.of(segmentResult1));
    when(writer2.complete()).thenReturn(ImmutableList.of(segmentResult2));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean()))
        .thenReturn(writer1)   // before first broadcast flag pair
        .thenReturn(writer2);  // between first and second broadcast flag pairs

    Worker worker = new Worker(config, writerFactory);

    Map<String, Object> rowValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME, "type", "END-LOAD");

    // Same flag type broadcast to partition 0 and 1 — interleaved batch from a single task.
    SinkRecord row_p0_1  = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, rowValue,  0L);
    SinkRecord row_p1_1  = new SinkRecord(SRC_TOPIC_NAME, 1, null, "key", null, rowValue,  0L);
    SinkRecord flag_p0_1 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    SinkRecord flag_p1_1 = new SinkRecord(SRC_TOPIC_NAME, 1, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    SinkRecord row_p0_2  = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, rowValue,  2L);
    SinkRecord row_p1_2  = new SinkRecord(SRC_TOPIC_NAME, 1, null, "key", null, rowValue,  2L);
    SinkRecord flag_p0_2 = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 3L);
    SinkRecord flag_p1_2 = new SinkRecord(SRC_TOPIC_NAME, 1, null, FLAG_PREFIX + "end", null, flagValue, 3L);

    worker.write(ImmutableList.of(
        row_p0_1, row_p1_1,
        flag_p0_1, flag_p1_1,
        row_p0_2, row_p1_2,
        flag_p0_2, flag_p1_2));

    List<WriterResult> results = worker.committable().writerResults();

    // Expected: [data_segment1, flag_p0(seqno=1), flag_p1(seqno=1), data_segment2, flag_p0(seqno=2), flag_p1(seqno=2)]
    assertThat(results).hasSize(6);
    assertThat(results.get(0)).isSameAs(segmentResult1);
    assertThat(results.get(1)).isInstanceOf(FlagWriterResult.class);
    assertThat(results.get(2)).isInstanceOf(FlagWriterResult.class);
    assertThat(results.get(3)).isSameAs(segmentResult2);
    assertThat(results.get(4)).isInstanceOf(FlagWriterResult.class);
    assertThat(results.get(5)).isInstanceOf(FlagWriterResult.class);

    // Both broadcast copies of the FIRST logical flag must carry seqno=1.
    // This is the core correctness property: the Coordinator accumulates ONE vote per partition
    // for seqno=1, reaching quorum (2) exactly once, and processing the flag once.
    FlagWriterResult fwrFirstP0 = (FlagWriterResult) results.get(1);
    FlagWriterResult fwrFirstP1 = (FlagWriterResult) results.get(2);
    String jsonFirstP0 = fwrFirstP0.dataFiles().get(0).path().toString()
        .substring(FlagWriterResult.FLAG_PREFIX.length());
    String jsonFirstP1 = fwrFirstP1.dataFiles().get(0).path().toString()
        .substring(FlagWriterResult.FLAG_PREFIX.length());
    assertThat(jsonFirstP0).contains("\"" + Worker.FLAG_SEQNO_FIELD + "\":1");
    assertThat(jsonFirstP1).contains("\"" + Worker.FLAG_SEQNO_FIELD + "\":1");

    // Both broadcast copies of the SECOND logical flag must carry seqno=2.
    FlagWriterResult fwrSecondP0 = (FlagWriterResult) results.get(4);
    FlagWriterResult fwrSecondP1 = (FlagWriterResult) results.get(5);
    String jsonSecondP0 = fwrSecondP0.dataFiles().get(0).path().toString()
        .substring(FlagWriterResult.FLAG_PREFIX.length());
    String jsonSecondP1 = fwrSecondP1.dataFiles().get(0).path().toString()
        .substring(FlagWriterResult.FLAG_PREFIX.length());
    assertThat(jsonSecondP0).contains("\"" + Worker.FLAG_SEQNO_FIELD + "\":2");
    assertThat(jsonSecondP1).contains("\"" + Worker.FLAG_SEQNO_FIELD + "\":2");
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
