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
   * Verifies that reroute persists across {@code committable()} calls and is only cleared
   * once {@code onFlagProcessed()} is called — as would happen when the Coordinator broadcasts
   * the reroute-clear sentinel after all partition votes have been processed.
   *
   * <p>Scenario: a flag arrives in cycle K; the reroute must stay active in cycle K+1 (before the
   * Coordinator has processed all votes) and must stop in cycle K+2 after {@code onFlagProcessed}.
   */
  @Test
  public void testReroutePersistsAcrossCommitCyclesUntilFlagProcessed() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    IcebergWriter dataWriter = mock(IcebergWriter.class);
    when(dataWriter.complete()).thenReturn(ImmutableList.of());
    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(dataWriter);

    Worker worker = new Worker(config, writerFactory);

    // --- Cycle K: flag arrives ---
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));
    // Simulate the coordinator sending START_COMMIT → worker.committable() is called
    Committable cycleK = worker.committable();
    assertThat(cycleK.writerResults()).as("Flag must be in cycle-K committable").hasSize(1);

    // --- Cycle K+1: Coordinator has NOT processed the flag yet ---
    // A data record whose natural route is "db.other" should be rerouted to TABLE_NAME
    String otherTable = "db.other";
    Map<String, Object> dataValue = ImmutableMap.of(FIELD_NAME, otherTable);
    SinkRecord dataRec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, dataValue, 2L);
    worker.write(ImmutableList.of(dataRec));
    // Reroute is still active: writer must be created for TABLE_NAME, not "db.other"
    verify(writerFactory, times(1)).createWriter(eq(TABLE_NAME), any(), anyBoolean());
    worker.committable(); // drain cycle K+1

    // --- Coordinator signals flag processed (simulates the sentinel CommitComplete) ---
    worker.onFlagProcessed();

    // --- Cycle K+2: reroute cleared, data should go to its own route ---
    String yetAnotherTable = "db.yet_another";
    Map<String, Object> dataValue2 = ImmutableMap.of(FIELD_NAME, yetAnotherTable);
    SinkRecord dataRec2 = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key2", null, dataValue2, 3L);
    worker.write(ImmutableList.of(dataRec2));
    // Reroute cleared: writer must be created for yetAnotherTable (not TABLE_NAME)
    verify(writerFactory, times(1)).createWriter(eq(yetAnotherTable), any(), anyBoolean());
  }

  /**
   * Mirrors the new {@link TaskImpl#put()} order: {@code onFlagProcessed()} is called BEFORE
   * {@code write()}, so the records written in the same call are NOT rerouted.
   * This verifies that the sentinel-then-write ordering eliminates the race window where
   * records would be unnecessarily routed to the flag branch after the branch switch.
   */
  @Test
  public void testRerouteIsClearedBeforeWriteWhenSentinelPrecedesWrite() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.dynamicTablesEnabled()).thenReturn(true);
    when(config.tablesRouteField()).thenReturn(FIELD_NAME);
    when(config.flagKeyPrefix()).thenReturn(FLAG_PREFIX);
    when(config.branchesRegexDelimiter()).thenReturn(null);

    IcebergWriter dataWriter = mock(IcebergWriter.class);
    when(dataWriter.complete()).thenReturn(ImmutableList.of());
    IcebergWriterFactory writerFactory = mock(IcebergWriterFactory.class);
    when(writerFactory.createWriter(any(), any(), anyBoolean())).thenReturn(dataWriter);

    Worker worker = new Worker(config, writerFactory);

    // Activate reroute by writing a flag record
    Map<String, Object> flagValue = ImmutableMap.of(FIELD_NAME, TABLE_NAME);
    SinkRecord flagRec = new SinkRecord(SRC_TOPIC_NAME, 0, null, FLAG_PREFIX + "end", null, flagValue, 1L);
    worker.write(ImmutableList.of(flagRec));
    worker.committable(); // end cycle K

    // Simulate the TaskImpl.put() order: sentinel first, then write
    worker.onFlagProcessed();   // committer.commit() detects sentinel BEFORE write()

    String naturalRoute = "db.natural";
    Map<String, Object> dataValue = ImmutableMap.of(FIELD_NAME, naturalRoute);
    SinkRecord dataRec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, dataValue, 2L);
    worker.write(ImmutableList.of(dataRec));

    // Record must be routed to its natural destination, not to TABLE_NAME
    verify(writerFactory, times(1)).createWriter(eq(naturalRoute), any(), anyBoolean());
    verify(writerFactory, times(0)).createWriter(eq(TABLE_NAME), any(), anyBoolean());
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
