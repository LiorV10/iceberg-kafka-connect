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
   * Flag on the correct (configured) source partition is accepted and staged as pending.
   * After a second write() call the pending flag is activated (barrier pattern).
   */
  @Test
  public void testFlagOnCorrectSourcePartitionIsAccepted() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.flagSourcePartition()).thenReturn(0);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    Worker worker = new Worker(config, writerFactory);

    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    // Batch 1: flag record on partition 0 (matches configured source partition)
    SinkRecord flagRec = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));

    // After batch 1, flag is PENDING — not yet activated
    assertThat(worker.committable().writerResults())
        .as("Flag should still be pending after the batch it arrived in")
        .isEmpty();

    // Batch 2 (empty): activatePendingFlags() runs at the start of write() and promotes the flag
    worker.write(ImmutableList.of());

    Committable committable = worker.committable();
    assertThat(committable.writerResults())
        .as("Flag should be activated after a subsequent write() call")
        .hasSize(1)
        .allMatch(r -> r instanceof FlagWriterResult);
  }

  /**
   * A flag-looking record that arrives on the wrong partition (when source-partition is
   * configured) must be treated as a regular data record and must NOT be staged as a flag.
   */
  @Test
  public void testFlagOnWrongSourcePartitionIsIgnoredAsFlag() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    // Flags must come from partition 0
    when(config.flagSourcePartition()).thenReturn(0);

    WriterResult dataWriteResult =
        new WriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(dataWriteResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Worker worker = new Worker(config, writerFactory);

    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    // Flag-looking record arrives on partition 1 — does NOT match the configured source partition 0
    SinkRecord wrongPartitionFlag = new SinkRecord(SRC_TOPIC_NAME, 1, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(wrongPartitionFlag));

    Committable committable = worker.committable();
    // The record should be treated as a data record, not a flag
    assertThat(committable.writerResults())
        .as("Record from wrong partition should be treated as data, not as a flag")
        .hasSize(1)
        .noneMatch(r -> r instanceof FlagWriterResult);
  }

  /**
   * When source-partition is -1 (default, any partition), flags on any partition are accepted.
   */
  @Test
  public void testFlagWithNoSourcePartitionConfiguredAcceptsAnyPartition() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    // -1 means any partition is accepted
    when(config.flagSourcePartition()).thenReturn(-1);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    Worker worker = new Worker(config, writerFactory);

    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    // Flag record on partition 3 — should be accepted because source partition is -1
    SinkRecord flagRec = new SinkRecord(SRC_TOPIC_NAME, 3, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));

    // Batch 1: flag staged as pending
    assertThat(worker.committable().writerResults()).isEmpty();

    // Batch 2: empty write triggers activation
    worker.write(ImmutableList.of());

    assertThat(worker.committable().writerResults())
        .hasSize(1)
        .allMatch(r -> r instanceof FlagWriterResult);
  }

  /**
   * Records from other partitions that arrive in the SAME batch as the flag must be
   * written to the original (non-rerouted) destination, not the flag's branch.
   * The reroute only kicks in for the NEXT write() batch.
   */
  @Test
  public void testRecordsFromOtherPartitionsInFlagBatchAreNotRerouted() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.flagSourcePartition()).thenReturn(0);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    WriterResult dataWriteResult =
        new WriterResult(
            TableIdentifier.parse(TABLE_NAME),
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(),
            StructType.of());
    IcebergWriter writer = mock(IcebergWriter.class);
    when(writer.complete()).thenReturn(ImmutableList.of(dataWriteResult));

    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(writer);

    Worker worker = new Worker(config, writerFactory);

    Map<String, Object> dataValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);

    // Batch 1: data from partition 1 followed by a flag on partition 0 in the same batch
    SinkRecord dataRec = new SinkRecord(SRC_TOPIC_NAME, 1, null, "pk1", null, dataValue, 0L);
    SinkRecord flagRec = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(dataRec, flagRec));

    Committable committable = worker.committable();
    // Only the data write result from partition 1 — no flag yet (still pending)
    assertThat(committable.writerResults())
        .as("Data from other partitions in same batch as flag should be written; flag still pending")
        .hasSize(1)
        .noneMatch(r -> r instanceof FlagWriterResult);
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
