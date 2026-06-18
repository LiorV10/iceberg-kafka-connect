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
 * A {@link TaskWriter} decorator that collapses records sharing the same identifier (primary key)
 * within a single commit down to a single, latest record (last write wins) before delegating to an
 * underlying {@link TaskWriter}.
 *
 * <p><b>Why this is needed.</b> In upsert / CDC-update mode every record is written by the
 * underlying delta writer as an equality delete (keyed on the primary key) plus an insert. An
 * equality delete only applies to data files whose data sequence number is strictly less than the
 * delete's, so within a single commit (a single sequence number) an equality delete cannot remove a
 * data row written in that same commit. The delta writer normally suppresses a same-batch duplicate
 * via a <i>positional</i> delete, by matching the earlier insert that is still present in its
 * in-memory {@code insertedRowMap}. However that bookkeeping does not reliably survive a rolling
 * data-file boundary: once the writer rolls to a new data file, the previous insert may no longer be
 * matchable, and the second occurrence of the key degrades to another equality delete. The result
 * is that two updates of the same key inside one commit can both survive as data rows (observed as
 * two data files + two equality deletes, with no positional delete), instead of the latest value
 * overriding the earlier one.
 *
 * <p><b>What this does.</b> By buffering records for the current commit keyed by their identifier
 * fields and only handing the <i>last</i> record per key to the delegate at {@link #complete()}
 * time, every key reaches the delta writer at most once per commit. There is therefore never a
 * second same-key row to suppress, so the rolling-file race cannot reintroduce duplicates and the
 * latest value always wins. This does not rely on, or change, sequence-number semantics, so a
 * normal single upsert (one occurrence of a key) is unaffected, and deletes that target rows from
 * earlier commits continue to work exactly as before.
 *
 * <p>When the table has no identifier fields (append-only / non-upsert), records are passed through
 * to the delegate unchanged.
 */
class DeduplicatedTaskWriter implements TaskWriter<Record> {

  private final TaskWriter<Record> delegate;
  private final InternalRecordWrapper wrapper;
  private final StructProjection keyProjection;
  private final boolean dedupEnabled;

  // StructLikeMap provides type-aware key equality over the identifier struct and materializes a
  // stable key copy on put(), so two records with the same logical key collapse to one entry.
  private final StructLikeMap<Record> buffer;

  DeduplicatedTaskWriter(
      TaskWriter<Record> delegate, Schema schema, Set<Integer> identifierFieldIds) {
    this.delegate = delegate;
    this.dedupEnabled = identifierFieldIds != null && !identifierFieldIds.isEmpty();
    if (dedupEnabled) {
      Schema deleteSchema = TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds));
      this.wrapper = new InternalRecordWrapper(schema.asStruct());
      this.keyProjection = StructProjection.create(schema, deleteSchema);
      this.buffer = StructLikeMap.create(deleteSchema.asStruct());
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

    // Project the identifier fields to form the key. StructLikeMap#put copies the key internally
    // (via StructLikeWrapper#copyFor), so reusing the projection wrapper across rows is safe.
    StructLike key = keyProjection.wrap(wrapper.wrap(row));
    // Last write wins: a later record for the same key replaces the earlier buffered record.
    buffer.put(key, row);
  }

  @Override
  public WriteResult complete() throws IOException {
    if (dedupEnabled) {
      for (Record row : buffer.values()) {
        delegate.write(row);
      }
      buffer.clear();
    }
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
}
