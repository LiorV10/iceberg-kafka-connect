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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;

/**
 * A {@link TaskWriter} that deduplicates records by their identifier (primary key) fields within a
 * single commit before delegating to an underlying {@link TaskWriter}.
 *
 * <p>This exists to fix a duplicate-row problem in upsert / CDC-update mode. In upsert mode every
 * record is written as a delete + insert, producing an equality delete keyed on the record's
 * primary key. Equality deletes only apply to data files whose sequence number is strictly less
 * than the delete's, so within a single commit (one sequence number) an equality delete cannot
 * remove a data row written in the same commit. The underlying Iceberg delta writer normally
 * suppresses a same-batch duplicate via a positional delete (matching the earlier insert in its
 * in-memory {@code insertedRowMap}), but that bookkeeping does not survive a rolling data-file
 * boundary, so two updates of the same key in one commit can be observed as two surviving data rows
 * (two data files + two equality deletes, no positional delete).
 *
 * <p>By collapsing all records for a given primary key down to a single final record (last write
 * wins, delete-aware) and only handing that single record to the underlying writer, every key
 * reaches the delta writer at most once per commit. There is therefore never a second same-key row
 * to suppress, and the rolling-file race cannot reintroduce duplicates. Records without identifier
 * fields are passed through unchanged so behavior for append-only / non-upsert tables is unaffected.
 */
class DeduplicatedTaskWriter implements TaskWriter<Record> {

  private final TaskWriter<Record> delegate;
  private final InternalRecordWrapper wrapper;
  private final StructProjection keyProjection;
  private final boolean dedupEnabled;

  // Insertion-ordered so that the records are replayed to the delegate in the order their keys were
  // first seen, while the value reflects the latest record for that key (last write wins).
  private final Map<StructLike, Record> buffer;

  DeduplicatedTaskWriter(
      TaskWriter<Record> delegate, Schema schema, Set<Integer> identifierFieldIds) {
    this.delegate = delegate;
    this.dedupEnabled = identifierFieldIds != null && !identifierFieldIds.isEmpty();
    if (dedupEnabled) {
      Schema deleteSchema = TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds));
      this.wrapper = new InternalRecordWrapper(schema.asStruct());
      this.keyProjection = StructProjection.create(schema, deleteSchema);
      this.buffer = StructLikeMapWithInsertionOrder.create(deleteSchema);
    } else {
      this.wrapper = null;
      this.keyProjection = null;
      this.buffer = null;
    }
  }

  @Override
  public void write(Record row) throws IOException {
    if (!dedupEnabled) {
      delegate.write(row);
      return;
    }

    // Build a stable key over the identifier fields. copyFor wraps a fresh, independent projection
    // backed by the row; we additionally rely on StructLikeMap to compute equality by value+type.
    StructLike key = keyProjection.copyFor(wrapper.wrap(row));
    // Last write wins. Removing first preserves the original key instance / ordering semantics of
    // the backing map while still updating to the latest record for replay.
    buffer.put(key, row);
  }

  /** Replays the deduplicated records into the delegate. */
  private void flushBuffer() throws IOException {
    if (!dedupEnabled) {
      return;
    }
    for (Record row : buffer.values()) {
      delegate.write(row);
    }
    buffer.clear();
  }

  @Override
  public WriteResult complete() throws IOException {
    flushBuffer();
    return delegate.complete();
  }

  @Override
  public void abort() throws IOException {
    if (dedupEnabled) {
      buffer.clear();
    }
    delegate.abort();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  /**
   * Thin wrapper that delegates to {@link StructLikeMap} but iterates values in insertion order.
   * StructLikeMap provides the type-aware key equality required to match identifier structs, but
   * does not guarantee iteration order; preserving first-seen order keeps replay deterministic.
   */
  private static final class StructLikeMapWithInsertionOrder {
    private StructLikeMapWithInsertionOrder() {}

    static Map<StructLike, Record> create(Schema deleteSchema) {
      return new OrderedStructLikeMap(deleteSchema);
    }
  }

  private static final class OrderedStructLikeMap extends java.util.AbstractMap<StructLike, Record> {
    private final Map<StructLike, Record> typeAware;
    private final java.util.LinkedHashSet<StructLike> order;

    OrderedStructLikeMap(Schema deleteSchema) {
      this.typeAware = StructLikeMap.create(deleteSchema.asStruct());
      this.order = new java.util.LinkedHashSet<>();
    }

    @Override
    public Record put(StructLike key, Record value) {
      Record previous = typeAware.put(key, value);
      // LinkedHashSet keeps the first-seen position stable across re-puts for the same key.
      order.add(key);
      return previous;
    }

    @Override
    public Collection<Record> values() {
      java.util.List<Record> ordered = new java.util.ArrayList<>(order.size());
      for (StructLike key : order) {
        ordered.add(typeAware.get(key));
      }
      return ordered;
    }

    @Override
    public void clear() {
      typeAware.clear();
      order.clear();
    }

    @Override
    public Set<Entry<StructLike, Record>> entrySet() {
      // Not used for replay, but provide a consistent view for completeness.
      Map<StructLike, Record> ordered = new LinkedHashMap<>();
      for (StructLike key : order) {
        ordered.put(key, typeAware.get(key));
      }
      return ordered.entrySet();
    }
  }
}
