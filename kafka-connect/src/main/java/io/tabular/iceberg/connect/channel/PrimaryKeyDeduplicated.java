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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes data files that contain the same primary key(s) as another data file in the same commit
 * batch.
 *
 * <p>This guards against the following failure mode: when the catalog (e.g. a REST catalog pod)
 * becomes unstable a commit can fail <em>after</em> a worker has durably written its data files to
 * object storage but <em>before</em> the source-topic offsets for that worker have been advanced.
 * Kafka Connect then restarts the task, rewinds to the last committed offsets, and re-processes the
 * same source records, writing them into <strong>brand-new</strong> data files (new paths). The
 * existing {@link Deduplicated} logic only collapses files that share the exact same path, so it
 * cannot recognize these as duplicates. The net effect is the same primary key appearing in two
 * data files within a single snapshot, which surfaces as duplicate rows on read because Iceberg
 * equality deletes do not apply to data files written in the same snapshot.
 *
 * <p>Because the same primary key is always routed to the same Kafka partition (and therefore the
 * same table partition, as the partition columns are a subset of the key), the duplicate files for
 * a given key share identical identifier-field lower/upper bounds. We use those bounds, recorded in
 * the {@link DataFile} metrics, to detect single-key files that collide and keep only the one
 * carrying the most-recently-written rows. "Most recent" is approximated by the control-topic
 * offset of the envelope that reported the file: a re-written file is reported in a later commit
 * cycle / control-topic offset than the original.
 *
 * <p>The dedup is intentionally conservative. It only collapses files whose identifier-field lower
 * bound equals their upper bound (i.e. files that contain exactly one distinct key) and whose
 * bounds are fully populated. Multi-key files are left untouched so that we never drop a file that
 * also contains non-duplicated keys.
 */
class PrimaryKeyDeduplicated {

  private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyDeduplicated.class);

  private PrimaryKeyDeduplicated() {}

  /**
   * Deduplicates the given data files by their identifier-field bounds.
   *
   * @param dataFiles the candidate data files, already deduplicated by path
   * @param fileOffsets the control-topic offset of the envelope that reported each data file, keyed
   *     by data-file path; used to decide which of two colliding files is newer
   * @param schema the table schema
   * @param identifierFieldIds the identifier (primary key) field ids; when empty this is a no-op
   * @return the surviving data files, with older fully-overlapping single-key files removed
   */
  static List<DataFile> dataFiles(
      List<DataFile> dataFiles,
      Map<String, Long> fileOffsets,
      Schema schema,
      Set<Integer> identifierFieldIds) {
    if (dataFiles.size() < 2 || identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      return dataFiles;
    }

    List<Integer> sortedIds = Lists.newArrayList(identifierFieldIds);
    sortedIds.sort(Comparator.naturalOrder());

    // bound-key -> surviving data file for that key
    Map<String, DataFile> winners = Maps.newLinkedHashMap();
    List<DataFile> result = Lists.newArrayList();

    for (DataFile dataFile : dataFiles) {
      String key = singleKeyBound(dataFile, sortedIds, schema);
      if (key == null) {
        // multi-key or missing-bounds file: never drop, keep as-is
        result.add(dataFile);
        continue;
      }

      DataFile existing = winners.get(key);
      if (existing == null) {
        winners.put(key, dataFile);
      } else {
        DataFile newer = newer(existing, dataFile, fileOffsets);
        DataFile older = newer == existing ? dataFile : existing;
        winners.put(key, newer);
        LOG.warn(
            "Dropping duplicate-PK data file path={} (recordCount={}, offset={}) in favor of "
                + "path={} (recordCount={}, offset={}); same identifier-field bounds={} appeared in "
                + "multiple data files within one commit, likely from a task restart re-writing "
                + "already-committed records",
            older.path(),
            older.recordCount(),
            fileOffsets.get(older.path().toString()),
            newer.path(),
            newer.recordCount(),
            fileOffsets.get(newer.path().toString()),
            key);
      }
    }

    result.addAll(winners.values());
    return result;
  }

  /**
   * Returns a stable string describing the single identifier-field value contained in the file, or
   * {@code null} if the file does not contain exactly one distinct key or its bounds are not fully
   * populated. A file contains exactly one distinct key when, for every identifier field, the lower
   * bound equals the upper bound.
   */
  private static String singleKeyBound(DataFile dataFile, List<Integer> sortedIds, Schema schema) {
    Map<Integer, ByteBuffer> lower = dataFile.lowerBounds();
    Map<Integer, ByteBuffer> upper = dataFile.upperBounds();
    if (lower == null || upper == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    for (Integer fieldId : sortedIds) {
      ByteBuffer lo = lower.get(fieldId);
      ByteBuffer hi = upper.get(fieldId);
      if (lo == null || hi == null) {
        return null;
      }

      Types.NestedField field = schema.findField(fieldId);
      if (field == null) {
        return null;
      }
      Type.PrimitiveType type = field.type().asPrimitiveType();

      Object loVal = org.apache.iceberg.types.Conversions.fromByteBuffer(type, lo);
      Object hiVal = org.apache.iceberg.types.Conversions.fromByteBuffer(type, hi);

      @SuppressWarnings("unchecked")
      Comparator<Object> cmp = (Comparator<Object>) Comparators.forType(type);
      if (cmp.compare(loVal, hiVal) != 0) {
        // more than one distinct value for this key column -> not a single-key file
        return null;
      }

      sb.append(fieldId).append('=').append(stringify(loVal)).append(';');
    }
    return sb.toString();
  }

  private static String stringify(Object value) {
    if (value instanceof ByteBuffer) {
      ByteBuffer buf = ((ByteBuffer) value).duplicate();
      byte[] bytes = new byte[buf.remaining()];
      buf.get(bytes);
      StringBuilder sb = new StringBuilder(bytes.length * 2);
      for (byte b : bytes) {
        sb.append(Character.forDigit((b >> 4) & 0xF, 16));
        sb.append(Character.forDigit(b & 0xF, 16));
      }
      return sb.toString();
    }
    if (value instanceof StructLike) {
      return String.valueOf(value);
    }
    return String.valueOf(value);
  }

  /** Returns whichever file was reported on a later control-topic offset (ties: higher recordCount). */
  private static DataFile newer(DataFile a, DataFile b, Map<String, Long> fileOffsets) {
    long offA = fileOffsets.getOrDefault(a.path().toString(), Long.MIN_VALUE);
    long offB = fileOffsets.getOrDefault(b.path().toString(), Long.MIN_VALUE);
    if (offA != offB) {
      return offA > offB ? a : b;
    }
    long cntA = a.recordCount();
    long cntB = b.recordCount();
    if (cntA != cntB) {
      return cntA >= cntB ? a : b;
    }
    // deterministic fallback: lexicographically greater path wins
    return a.path().toString().compareTo(b.path().toString()) >= 0 ? a : b;
  }
}
