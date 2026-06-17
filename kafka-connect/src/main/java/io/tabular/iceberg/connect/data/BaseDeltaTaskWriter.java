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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
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

abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final InternalRecordWrapper wrapper;
  private final InternalRecordWrapper keyWrapper;
  private final RecordProjection keyProjection;
  private final boolean upsertMode;
  private final Map<KeyAndWriter, PendingChange> pendingChanges = new LinkedHashMap<>();

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
  }

  abstract RowDataDeltaWriter route(Record row);

  InternalRecordWrapper wrapper() {
    return wrapper;
  }

  @Override
  public void write(Record row) throws IOException {
    Operation op =
        row instanceof RecordWrapper
            ? ((RecordWrapper) row).op()
            : upsertMode ? Operation.UPDATE : Operation.INSERT;

    RowDataDeltaWriter writer = route(row);
    KeyAndWriter mapKey = new KeyAndWriter(writer, Key.copyOf(keyProjection.wrap(row)));
    PendingChange pending = pendingChanges.get(mapKey);
    if (pending == null) {
      pending = new PendingChange(writer, mapKey.key(), op, op == Operation.DELETE ? null : row.copy());
      pendingChanges.put(mapKey, pending);
      return;
    }

    pending.merge(op, row);
  }

  @Override
  public void close() throws IOException {
    flushPendingChanges();
  }

  protected void flushPendingChanges() throws IOException {
    for (PendingChange pending : pendingChanges.values()) {
      pending.apply();
    }
    pendingChanges.clear();
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

  private static class PendingChange {
    private final RowDataDeltaWriter writer;
    private final Key key;
    private boolean requiresDelete;
    private Record rowToWrite;

    private PendingChange(RowDataDeltaWriter writer, Key key, Operation op, Record rowToWrite) {
      this.writer = writer;
      this.key = key;
      this.requiresDelete = op == Operation.UPDATE || op == Operation.DELETE;
      this.rowToWrite = rowToWrite;
    }

    private void merge(Operation op, Record row) {
      switch (op) {
        case INSERT:
          rowToWrite = row.copy();
          break;
        case UPDATE:
          requiresDelete = true;
          rowToWrite = row.copy();
          break;
        case DELETE:
          requiresDelete = true;
          rowToWrite = null;
          break;
        default:
          throw new IllegalArgumentException("Unsupported operation: " + op);
      }
    }

    private void apply() throws IOException {
      if (requiresDelete) {
        writer.deleteKey(key);
      }
      if (rowToWrite != null) {
        writer.write(rowToWrite);
      }
    }
  }

  private static class KeyAndWriter {
    private final RowDataDeltaWriter writer;
    private final Key key;

    private KeyAndWriter(RowDataDeltaWriter writer, Key key) {
      this.writer = writer;
      this.key = key;
    }

    private Key key() {
      return key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KeyAndWriter that = (KeyAndWriter) o;
      return writer == that.writer && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
      return 31 * System.identityHashCode(writer) + key.hashCode();
    }
  }

  private static class Key implements StructLike {
    private final Object[] values;

    private Key(Object[] values) {
      this.values = values;
    }

    private static Key copyOf(Record record) {
      Object[] values = new Object[record.size()];
      for (int i = 0; i < record.size(); i += 1) {
        values[i] = deepCopy(record.get(i));
      }
      return new Key(values);
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return (T) values[pos];
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key key = (Key) o;
      return java.util.Arrays.deepEquals(values, key.values);
    }

    @Override
    public int hashCode() {
      return java.util.Arrays.deepHashCode(values);
    }

    private static Object deepCopy(Object value) {
      if (value instanceof Record) {
        return ((Record) value).copy();
      }
      if (value instanceof java.util.List) {
        java.util.List<?> list = (java.util.List<?>) value;
        java.util.List<Object> copy = new java.util.ArrayList<>(list.size());
        for (Object element : list) {
          copy.add(deepCopy(element));
        }
        return copy;
      }
      if (value instanceof java.util.Map) {
        java.util.Map<?, ?> map = (java.util.Map<?, ?>) value;
        java.util.Map<Object, Object> copy = new java.util.LinkedHashMap<>();
        for (java.util.Map.Entry<?, ?> entry : map.entrySet()) {
          copy.put(deepCopy(entry.getKey()), deepCopy(entry.getValue()));
        }
        return copy;
      }
      return value;
    }
  }
}
