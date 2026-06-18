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
package io.tabular.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.TableSinkConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests the per-key deduplication behavior of {@link DeduplicatedTaskWriter} in upsert mode,
 * including the operation-aware (insert / update / delete) cases driven through {@link
 * RecordWrapper}. These go through {@link Utilities#createTableWriter} so the full wiring is
 * exercised end to end.
 */
public class DeduplicatedTaskWriterTest extends BaseWriterTest {

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testInsertThenDeleteSameKeyInBatchProducesNoRow(String format) {
    // CDC mode (a cdc field is configured): the operations are taken from the RecordWrapper.
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.upsertModeEnabled()).thenReturn(false);
    when(config.tablesCdcField()).thenReturn("_cdc_op");
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));

    // Insert a new key then delete it in the same commit. The final state is deleted, so no data
    // row should survive.
    Record insert = GenericRecord.create(SCHEMA);
    insert.setField("id", 123L);
    insert.setField("data", "v1");
    insert.setField("id2", 123L);

    Record delete = GenericRecord.create(SCHEMA);
    delete.setField("id", 123L);
    delete.setField("data", "v1");
    delete.setField("id2", 123L);

    WriteResult result =
        writeTest(
            ImmutableList.of(
                new RecordWrapper(insert, Operation.INSERT),
                new RecordWrapper(delete, Operation.DELETE)),
            config,
            UnpartitionedDeltaWriter.class);

    assertThat(totalRecordCount(ImmutableList.copyOf(result.dataFiles()))).isEqualTo(0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testDeleteThenInsertSameKeyInBatchProducesSingleRow(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.upsertModeEnabled()).thenReturn(false);
    when(config.tablesCdcField()).thenReturn("_cdc_op");
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));

    // Delete a key then re-insert it in the same commit (last write wins -> a row survives).
    Record delete = GenericRecord.create(SCHEMA);
    delete.setField("id", 123L);
    delete.setField("data", "old");
    delete.setField("id2", 123L);

    Record insert = GenericRecord.create(SCHEMA);
    insert.setField("id", 123L);
    insert.setField("data", "new");
    insert.setField("id2", 123L);

    WriteResult result =
        writeTest(
            ImmutableList.of(
                new RecordWrapper(delete, Operation.DELETE),
                new RecordWrapper(insert, Operation.INSERT)),
            config,
            UnpartitionedDeltaWriter.class);

    assertThat(totalRecordCount(ImmutableList.copyOf(result.dataFiles()))).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testDistinctKeysAreNotDeduplicated(String format) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.upsertModeEnabled()).thenReturn(true);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));

    Record row1 = GenericRecord.create(SCHEMA);
    row1.setField("id", 1L);
    row1.setField("data", "a");
    row1.setField("id2", 1L);

    Record row2 = GenericRecord.create(SCHEMA);
    row2.setField("id", 2L);
    row2.setField("data", "b");
    row2.setField("id2", 2L);

    WriteResult result =
        writeTest(ImmutableList.of(row1, row2), config, UnpartitionedDeltaWriter.class);

    // Two distinct keys -> two rows survive.
    assertThat(totalRecordCount(ImmutableList.copyOf(result.dataFiles()))).isEqualTo(2);
  }

  @ParameterizedTest
  @ValueSource(strings = {"parquet", "orc"})
  public void testAppendOnlyTableIsNotDeduplicated(String format) {
    // No identifier fields -> plain append writer, records must pass through unchanged (duplicates
    // of the "same row" are all retained, since there is no primary key to dedup on).
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.upsertModeEnabled()).thenReturn(false);
    when(config.tableConfig(any())).thenReturn(mock(TableSinkConfig.class));
    when(config.writeProps()).thenReturn(ImmutableMap.of("write.format.default", format));

    // Table whose schema has no identifier fields.
    org.apache.iceberg.Schema noIdSchema =
        new org.apache.iceberg.Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.LongType.get()),
            org.apache.iceberg.types.Types.NestedField.required(
                2, "data", org.apache.iceberg.types.Types.StringType.get()));
    when(table.schema()).thenReturn(noIdSchema);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());

    Record row1 = GenericRecord.create(noIdSchema);
    row1.setField("id", 7L);
    row1.setField("data", "dup");

    Record row2 = GenericRecord.create(noIdSchema);
    row2.setField("id", 7L);
    row2.setField("data", "dup");

    WriteResult result =
        writeTest(ImmutableList.of(row1, row2), config, UnpartitionedWriterClass());

    // Append-only: both rows are written, nothing is deduplicated.
    assertThat(totalRecordCount(ImmutableList.copyOf(result.dataFiles()))).isEqualTo(2);
  }

  /**
   * The concrete plain-append unpartitioned writer class used by {@link Utilities}. Resolved
   * reflectively-by-name only to avoid importing the Iceberg-internal generic type in the test
   * signature; it is {@code org.apache.iceberg.io.UnpartitionedWriter}.
   */
  private static Class<?> UnpartitionedWriterClass() {
    return org.apache.iceberg.io.UnpartitionedWriter.class;
  }

  // Overload to allow passing a List of RecordWrappers (which are Records) without unchecked casts.
  private WriteResult writeTest(
      List<? extends Record> rows, IcebergSinkConfig config, Class<?> expectedWriterClass) {
    try (TaskWriter<Record> writer = Utilities.createTableWriter(table, "name", config)) {
      assertThat(unwrapDelegate(writer)).isInstanceOf(expectedWriterClass);
      for (Record row : rows) {
        try {
          writer.write(row);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      return writer.complete();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static TaskWriter<Record> unwrapDelegate(TaskWriter<Record> writer) {
    if (writer instanceof DeduplicatedTaskWriter) {
      return ((DeduplicatedTaskWriter) writer).delegate();
    }
    return writer;
  }

  private static java.lang.annotation.Annotation any() {
    return null;
  }
}
