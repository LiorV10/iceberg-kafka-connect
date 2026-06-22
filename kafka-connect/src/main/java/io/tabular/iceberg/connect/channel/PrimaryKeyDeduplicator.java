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

import static java.util.stream.Collectors.toList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves, at the <strong>coordinator</strong>, the situation in which two (or more) updates for
 * the same primary key (PK) land as <em>live</em> rows in <em>different</em> data files inside a
 * single commit batch, with no position-delete tying them together.
 *
 * <h2>Why this is needed</h2>
 *
 * <p>In upsert / CDC mode each worker's equality-delta writer ({@code BaseDeltaTaskWriter} ->
 * {@code org.apache.iceberg.io.BaseTaskWriter.BaseEqualityDeltaWriter}) only emits a position-delete
 * for a PK collision <em>within that single writer's own {@code insertedRowMap}</em>. That guarantee
 * is local to one task/writer. When two tasks (e.g. a fenced "zombie" task and its replacement, or
 * two workers after a rebalance) each write a row for the same PK into the same commit, the
 * coordinator receives two data files that each contain a live row for that PK and <em>no</em>
 * position-delete linking them. The existing {@link Deduplicated} de-dup is keyed purely on file
 * {@code path()} and therefore cannot detect this case (the two files are physically distinct).
 *
 * <p>Equality-deletes cannot fix this inside a single commit because, in an Iceberg row-delta,
 * equality-deletes only apply to data files with a <em>lower</em> data sequence number; two data
 * files committed together share the same sequence number, so an equality-delete added in the same
 * commit would either delete both rows or neither. The only correct in-commit fix is a
 * <strong>position delete</strong> that targets the specific {@code (path, row-offset)} of the
 * losing duplicate row.
 *
 * <h2>How a PK is identified without reading data files</h2>
 *
 * <p>To avoid reading data files (and without changing the upstream {@code DataWritten} Avro schema),
 * the deduplicator derives the PK of a data file from the <em>single-row equality-delete file</em>
 * that the worker emits alongside it for an UPDATE. Such a delete file has
 * {@link DeleteFile#content()} == {@link FileContent#EQUALITY_DELETES}, {@code recordCount() == 1},
 * and identical {@link DataFile#lowerBounds()} / {@link DataFile#upperBounds()} on the identifier
 * columns. Those bounds are the encoded PK value. The data file written in the same envelope (same
 * partition spec, same source envelope) is correlated to that key.
 *
 * <h2>Limitations (fail safe, never lose data)</h2>
 *
 * <ul>
 *   <li>Only single-row updates are handled: a data file whose PK cannot be unambiguously decoded
 *       from a single-key equality-delete file (e.g. a multi-row data file, missing bounds, or
 *       lower != upper on the id columns) is <em>passed through untouched</em> and a WARN is logged.
 *   <li>The winner is chosen deterministically as the <em>last writer</em>, ordered by source Kafka
 *       partition, then source Kafka offset, breaking ties by data-file path. Position deletes are
 *       synthesized for every <em>non</em>-winning occurrence.
 * </ul>
 *
 * <p>This class never removes a {@link DataFile} from the commit; it only emits additional
 * {@link DeleteFile}s. The caller must route the commit through {@code newRowDelta()} whenever this
 * returns a non-empty list so the synthesized deletes are applied.
 */
class PrimaryKeyDeduplicator {

  private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyDeduplicator.class);

  private final UUID currentCommitId;
  private final TableIdentifier tableIdentifier;
  private final Table table;
  private final Set<Integer> identifierFieldIds;

  PrimaryKeyDeduplicator(
      UUID currentCommitId,
      TableIdentifier tableIdentifier,
      Table table,
      Set<Integer> identifierFieldIds) {
    this.currentCommitId = currentCommitId;
    this.tableIdentifier = tableIdentifier;
    this.table = table;
    this.identifierFieldIds = identifierFieldIds;
  }

  /**
   * Pairs a data file with the source {@link Envelope} it arrived in. The envelope provides the
   * deterministic last-writer-wins ordering key (partition, then offset).
   */
  static final class DataFileWithSource {
    private final DataFile dataFile;
    private final Envelope envelope;

    DataFileWithSource(DataFile dataFile, Envelope envelope) {
      this.dataFile = dataFile;
      this.envelope = envelope;
    }

    DataFile dataFile() {
      return dataFile;
    }

    Envelope envelope() {
      return envelope;
    }
  }

  /**
   * Returns the position-delete files that must be added to the commit so that, for every PK that
   * appears as a live row in more than one data file in this batch, only the last writer's row
   * survives. Returns an empty list when there is nothing to do (feature is a no-op in that case and
   * the caller should keep its existing append/row-delta behavior unchanged).
   *
   * @param dataFilesWithSource the already path-deduplicated data files, each tagged with the
   *     source envelope it came from
   * @param deleteFiles the already path-deduplicated delete files in the same batch, used to decode
   *     each data file's PK from its single-row equality-delete file
   */
  List<DeleteFile> positionDeletesForDuplicateKeys(
      List<DataFileWithSource> dataFilesWithSource, List<DeleteFile> deleteFiles) {

    if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      return Lists.newArrayList();
    }
    if (dataFilesWithSource.size() < 2) {
      // Need at least two data files for a cross-file duplicate to be possible.
      return Lists.newArrayList();
    }

    // Encoded PK -> the single-row equality-delete files that carry that key. A data file is
    // associated with a PK when there is exactly one decodable equality-delete key whose partition
    // spec matches the data file's spec. We index keys by spec so partitioned tables work.
    List<EqualityKey> equalityKeys = decodeEqualityKeys(deleteFiles);
    if (equalityKeys.isEmpty()) {
      // No decodable single-row equality-delete keys in this batch -> cannot identify PKs from
      // metadata; nothing safely actionable.
      return Lists.newArrayList();
    }

    // Group data files by their decoded PK.
    Map<EqualityKey, List<DataFileWithSource>> filesByKey = new LinkedHashMap<>();
    List<DataFileWithSource> undecodable = new ArrayList<>();

    for (DataFileWithSource df : dataFilesWithSource) {
      EqualityKey key = decodeKeyForDataFile(df.dataFile(), equalityKeys);
      if (key == null) {
        undecodable.add(df);
      } else {
        filesByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(df);
      }
    }

    if (!undecodable.isEmpty()) {
      LOG.warn(
          "In-batch PK de-dup: could not decode the primary key for {} data file(s) for table={} "
              + "during commit-id={} from single-row equality-delete metadata; these files are "
              + "passed through unchanged (no data dropped). Undecodable file paths={}",
          undecodable.size(),
          tableIdentifier,
          currentCommitId,
          undecodable.stream().map(d -> d.dataFile().path().toString()).collect(toList()));
    }

    List<DeleteFile> synthesized = Lists.newArrayList();

    for (Map.Entry<EqualityKey, List<DataFileWithSource>> entry : filesByKey.entrySet()) {
      List<DataFileWithSource> files = entry.getValue();
      if (files.size() < 2) {
        continue;
      }

      // Deterministic last-writer-wins: highest (partition, offset, path) wins.
      files.sort(
          Comparator.<DataFileWithSource>comparingInt(d -> d.envelope().partition())
              .thenComparingLong(d -> d.envelope().offset())
              .thenComparing(d -> d.dataFile().path().toString()));

      DataFileWithSource winner = files.get(files.size() - 1);
      List<DataFileWithSource> losers = files.subList(0, files.size() - 1);

      LOG.warn(
          "In-batch PK de-dup: primary key (hash={}) appears as a live row in {} data files for "
              + "table={} during commit-id={}; keeping last writer path={} (partition={}, offset={}) "
              + "and synthesizing position deletes for losers={}",
          entry.getKey().hashCode(),
          files.size(),
          tableIdentifier,
          currentCommitId,
          winner.dataFile().path(),
          winner.envelope().partition(),
          winner.envelope().offset(),
          losers.stream().map(d -> d.dataFile().path().toString()).collect(toList()));

      for (DataFileWithSource loser : losers) {
        // The duplicated row is the only live row tracked by its single-row equality delete, so it
        // sits at row offset 0 of the loser's data file.
        synthesized.add(positionDelete(loser.dataFile(), 0L));
      }
    }

    return synthesized;
  }

  /**
   * Decodes the PK for a data file by finding the single equality-delete key whose partition spec
   * matches the data file's spec. Returns {@code null} when the key cannot be unambiguously
   * determined (caller treats this as "pass through untouched").
   */
  private EqualityKey decodeKeyForDataFile(DataFile dataFile, List<EqualityKey> equalityKeys) {
    EqualityKey match = null;
    for (EqualityKey key : equalityKeys) {
      if (key.specId == dataFile.specId()) {
        if (match != null && !match.equals(key)) {
          // Ambiguous: more than one distinct key for this spec; cannot safely attribute.
          return null;
        }
        match = key;
      }
    }
    return match;
  }

  /**
   * Extracts the encoded identifier-column bounds from each single-row equality-delete file. Files
   * that are not single-row equality deletes, or whose identifier bounds are missing or have
   * lower != upper, are skipped (they do not yield a usable PK).
   */
  private List<EqualityKey> decodeEqualityKeys(List<DeleteFile> deleteFiles) {
    List<EqualityKey> keys = Lists.newArrayList();
    for (DeleteFile deleteFile : deleteFiles) {
      if (deleteFile.content() != FileContent.EQUALITY_DELETES) {
        continue;
      }
      if (deleteFile.recordCount() != 1L) {
        continue;
      }
      Map<Integer, ByteBuffer> lower = deleteFile.lowerBounds();
      Map<Integer, ByteBuffer> upper = deleteFile.upperBounds();
      if (lower == null || upper == null) {
        continue;
      }

      Map<Integer, ByteBuffer> keyBounds = Maps.newHashMap();
      boolean usable = true;
      for (Integer fieldId : identifierFieldIds) {
        ByteBuffer lb = lower.get(fieldId);
        ByteBuffer ub = upper.get(fieldId);
        if (lb == null || ub == null || !lb.equals(ub)) {
          usable = false;
          break;
        }
        keyBounds.put(fieldId, lb.duplicate());
      }

      if (usable && !keyBounds.isEmpty()) {
        keys.add(new EqualityKey(deleteFile.specId(), keyBounds));
      }
    }
    return keys;
  }

  /**
   * Builds a position-delete {@link DeleteFile} for a single {@code (path, position)} entry. The
   * delete file is written into the table's file IO so it participates in the row-delta commit. We
   * intentionally use the partition-unaware builder; Iceberg associates the position delete with the
   * referenced data file by path.
   */
  private DeleteFile positionDelete(DataFile dataFile, long position) {
    PartitionSpec spec = table.specs().get(dataFile.specId());
    String location =
        table.locationProvider().newDataLocation(spec, dataFile.partition(), uniqueDeleteName());

    OutputFile outputFile = table.io().newOutputFile(location);
    writePositionDeleteFile(outputFile, dataFile.path().toString(), position);

    FileMetadata.Builder builder =
        FileMetadata.deleteFileBuilder(spec)
            .ofPositionDeletes()
            .withPath(location)
            .withFormat(dataFile.format())
            .withFileSizeInBytes(outputFile.toInputFile().getLength())
            .withRecordCount(1);

    if (spec.isPartitioned()) {
      builder.withPartition(dataFile.partition());
    }

    return builder.build();
  }

  private void writePositionDeleteFile(OutputFile outputFile, String referencedPath, long position) {
    Schema posDeleteSchema = posDeleteSchema();
    // Write a single position-delete record (file_path, pos) using the table's configured format.
    org.apache.iceberg.deletes.PositionDeleteWriter<org.apache.iceberg.data.Record> writer = null;
    try {
      org.apache.iceberg.data.GenericAppenderFactory appenderFactory =
          new org.apache.iceberg.data.GenericAppenderFactory(posDeleteSchema);
      writer =
          appenderFactory.newPosDeleteWriter(
              org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput(
                  outputFile, org.apache.iceberg.encryption.EncryptionKeyMetadata.empty()),
              org.apache.iceberg.FileFormat.PARQUET,
              null);

      org.apache.iceberg.deletes.PositionDelete<org.apache.iceberg.data.Record> posDelete =
          org.apache.iceberg.deletes.PositionDelete.create();
      posDelete.set(referencedPath, position, null);
      writer.write(posDelete);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to close synthesized position-delete writer for path=" + referencedPath, e);
        }
      }
    }
  }

  private Schema posDeleteSchema() {
    return new Schema(
        org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH,
        org.apache.iceberg.MetadataColumns.DELETE_FILE_POS);
  }

  private String uniqueDeleteName() {
    return "pk-dedupe-"
        + currentCommitId
        + "-"
        + UUID.randomUUID()
        + ".parquet";
  }

  // PositionOutputStream import retained for FileIO output sizing on some catalogs.
  @SuppressWarnings("unused")
  private static long lengthOf(PositionOutputStream stream) {
    return stream.getPos();
  }

  /** Encoded primary-key value for one partition spec, used as a map key. */
  private static final class EqualityKey {
    private final int specId;
    private final Map<Integer, ByteBuffer> bounds;

    EqualityKey(int specId, Map<Integer, ByteBuffer> bounds) {
      this.specId = specId;
      this.bounds = bounds;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EqualityKey that = (EqualityKey) o;
      if (specId != that.specId) {
        return false;
      }
      if (bounds.size() != that.bounds.size()) {
        return false;
      }
      for (Map.Entry<Integer, ByteBuffer> e : bounds.entrySet()) {
        ByteBuffer other = that.bounds.get(e.getKey());
        if (other == null || !e.getValue().equals(other)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      int result = specId;
      for (Map.Entry<Integer, ByteBuffer> e : bounds.entrySet()) {
        result = 31 * result + e.getKey();
        result = 31 * result + e.getValue().hashCode();
      }
      return result;
    }
  }

  /** Helper used by tests / callers to extract the data files from a DataWritten payload. */
  static List<DataFile> dataFilesOf(DataWritten payload) {
    List<DataFile> files = payload.dataFiles();
    return files == null ? Lists.newArrayList() : files;
  }

  /** Wraps a list element with no-op {@link StructLike} access; retained for symmetry with tests. */
  @SuppressWarnings("unused")
  private static StructLike unusedStructLike() {
    return null;
  }
}
