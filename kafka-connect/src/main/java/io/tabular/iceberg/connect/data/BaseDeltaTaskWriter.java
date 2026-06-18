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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.iceberg.util.StructProjection;

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final InternalRecordWrapper wrapper;
  private final InternalRecordWrapper keyWrapper;
  private final RecordProjection keyProjection;
  private final boolean upsertMode;
  private final StructProjection keyStructProjection;
  private final Map<StructLike, BufferedRecord> rowBuffer;

  BaseDeltaTaskWriter(
      PartitionSpec spec,
      FileFormat format,
      FileAppenderFactory<Record> appenderFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Set<Integer> identifierFieldIds,
      boolean upsertMode) {
    super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds));
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
    this.keyProjection = RecordProjection.create(schema, deleteSchema);
    this.upsertMode = upsertMode;
    this.keyStructProjection = StructProjection.create(schema, deleteSchema);
    this.rowBuffer = StructLikeMap.create(deleteSchema.asStruct());
  }

  abstract RowDataDeltaWriter route(Record row);

  InternalRecordWrapper wrapper() {
    return wrapper;
  }

  /**
   * Buffers the incoming record keyed by its identifier (primary key) fields so that multiple
   * mutations of the same key within a single batch collapse to a single final operation
   * (last-write-wins, delete-aware).
   *
   * <p>Without this buffering, two records with the same primary key that route to different {@link
   * RowDataDeltaWriter}s (for example because their partition value differs, or in the partitioned
   * writer in general) each maintain an independent {@code insertedRowMap} inside Iceberg's {@code
   * BaseEqualityDeltaWriter}. As a result neither writer can convert the other's insert into a
   * positional delete, so both fall back to equality deletes. Equality deletes do not apply to data
   * files written with the same sequence number in the same commit, so both rows survive and the
   * key is effectively inserted twice instead of overriding. By collapsing per key before routing,
   * every key reaches the underlying writers at most once and this failure mode cannot occur.
   */
  @Override
  public void write(Record row) throws IOException {
    Operation op =
        row instanceof RecordWrapper
            ? ((RecordWrapper) row).op()
            : upsertMode ? Operation.UPDATE : Operation.INSERT;

    // Copy the key so buffered entries are stable even if the source record is mutated/reused.
    StructLike key = StructLikeUtil.copy(keyStructProjection.wrap(wrapper.wrap(row)));
    rowBuffer.put(key, new BufferedRecord(row, op));
  }

  /**
   * Flushes the deduplicated batch through the underlying delta writers. Each buffered key produces
   * at most one delete + append (or a single equality delete when its final state is a delete).
   */
  void flushRowBuffer() throws IOException {
    for (BufferedRecord buffered : rowBuffer.values()) {
      writeInternal(buffered.row(), buffered.op());
    }
    rowBuffer.clear();
  }

  private void writeInternal(Record row, Operation op) throws IOException {
    RowDataDeltaWriter writer = route(row);
    if (op == Operation.UPDATE || op == Operation.DELETE) {
      writer.deleteKey(keyProjection.wrap(row));
    }
    if (op == Operation.UPDATE || op == Operation.INSERT) {
      writer.write(row);
    }
  }

  private static class BufferedRecord {
    private final Record row;
    private final Operation op;

    BufferedRecord(Record row, Operation op) {
      this.row = row;
      this.op = op;
    }

    Record row() {
      return row;
    }

    Operation op() {
      return op;
    }
  }

  class RowDataDeltaWriter extends BaseEqualityDeltaWriter {

    RowDataDeltaWriter(PartitionKey partition) {
      super(partition, schema, deleteSchema);
    }

    @Override
    protected StructLike asStructLike(Record data) {
      return wrapper.wrap(data);
    }

    @Override
    protected StructLike asStructLikeKey(Record data) {
      return keyWrapper.wrap(data);
    }
  }
}
