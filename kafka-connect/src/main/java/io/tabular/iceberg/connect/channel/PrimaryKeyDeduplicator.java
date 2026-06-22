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
import static java.util.stream.Collectors.toSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.DataReader;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.EqualityDeleteWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.types.TypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs true, row-level primary-key de-duplication across multiple data files within a single
 * Iceberg commit, using the equality-deletes strategy ("Option A").
 *
 * <p>The coordinator may receive multiple data files in one commit that contain rows with the same
 * primary key (for example two workers writing the same upserted key, or a re-key/CDC stream). The
 * old metadata-only heuristic could not detect this because {@link DataFile} objects carry no row
 * contents. This class reads the actual primary-key tuples out of each incoming data file, detects
 * keys that appear in more than one file, and produces equality delete files that remove the older
 * duplicate rows so that only the newest row for each key survives.
 *
 * <h2>Recency / winner selection</h2>
 *
 * The input {@code dataFiles} list is ordered by the coordinator in control-topic (Kafka) offset
 * order. We therefore treat the <em>last</em> occurrence of a primary key in that ordering as the
 * winner (newest), and emit equality deletes for every <em>earlier</em> file that contained the
 * same key.
 *
 * <h2>Sequence-number correctness</h2>
 *
 * Equality deletes only remove rows whose data sequence number is strictly <em>less than</em> the
 * delete's sequence number. Because all files are committed together, the caller must commit the
 * generated equality deletes at a lower sequence number than the surviving (winning) data file. The
 * {@link Result} returned here separates the winning data files from the equality deletes so the
 * caller can order the commit accordingly (see {@code Coordinator#commitDeduplicated}). This class
 * does not itself commit anything.
 *
 * <h2>Safety</h2>
 *
 * De-duplication only activates when the {@code lakers.id-cols} table property is present and
 * non-empty. Any failure while reading files or writing delete files is swallowed (logged) and the
 * original, un-deduplicated data files are returned, so a dedup failure never aborts a commit.
 */
class PrimaryKeyDeduplicator {

  static final String ID_COLS_PROP = "lakers.id-cols";

  private static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyDeduplicator.class);

  private PrimaryKeyDeduplicator() {}

  /** Result of de-duplication: the data files to append plus any generated equality delete files. */
  static class Result {
    private final List<DataFile> dataFiles;
    private final List<DeleteFile> equalityDeletes;

    Result(List<DataFile> dataFiles, List<DeleteFile> equalityDeletes) {
      this.dataFiles = dataFiles;
      this.equalityDeletes = equalityDeletes;
    }

    List<DataFile> dataFiles() {
      return dataFiles;
    }

    List<DeleteFile> equalityDeletes() {
      return equalityDeletes;
    }

    boolean hasDeletes() {
      return !equalityDeletes.isEmpty();
    }
  }

  /**
   * De-duplicate the supplied data files on the primary key defined by the {@code lakers.id-cols}
   * table property.
   *
   * @param commitId the current commit id (for logging only)
   * @param tableIdentifier the table being committed to (for logging only)
   * @param table the loaded Iceberg table
   * @param dataFiles the data files for this commit, in control-topic offset order
   * @return a {@link Result} containing the data files to append and any equality delete files; when
   *     no de-duplication is needed (or it is disabled / fails) the original data files are returned
   *     with an empty delete list
   */
  static Result deduplicate(
      UUID commitId, TableIdentifier tableIdentifier, Table table, List<DataFile> dataFiles) {
    if (dataFiles == null || dataFiles.size() <= 1) {
      return new Result(dataFiles, Lists.newArrayList());
    }

    String idColsProp = table.properties().get(ID_COLS_PROP);
    if (idColsProp == null || idColsProp.trim().isEmpty()) {
      // dedup disabled for this table; behave exactly like the append-only path
      return new Result(dataFiles, Lists.newArrayList());
    }

    List<String> idColNames =
        Arrays.stream(idColsProp.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(toList());
    if (idColNames.isEmpty()) {
      return new Result(dataFiles, Lists.newArrayList());
    }

    Set<Integer> pkFieldIds;
    try {
      pkFieldIds =
          idColNames.stream()
              .map(
                  colName -> {
                    if (table.schema().findField(colName) == null) {
                      throw new IllegalArgumentException(
                          "Primary key column not found in schema: " + colName);
                    }
                    return table.schema().findField(colName).fieldId();
                  })
              .collect(toSet());
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "Skipping coordinator PK dedup for table {}: invalid {}={} ({})",
          tableIdentifier,
          ID_COLS_PROP,
          idColsProp,
          e.getMessage());
      return new Result(dataFiles, Lists.newArrayList());
    }

    try {
      return deduplicateInternal(
          commitId, tableIdentifier, table, dataFiles, idColNames, pkFieldIds);
    } catch (Exception e) {
      // Never let dedup abort a commit; fall back to committing the original data files.
      LOG.warn(
          "Coordinator PK dedup failed for table {} during commit {}; "
              + "falling back to committing original data files without dedup",
          tableIdentifier,
          commitId,
          e);
      return new Result(dataFiles, Lists.newArrayList());
    }
  }

  private static Result deduplicateInternal(
      UUID commitId,
      TableIdentifier tableIdentifier,
      Table table,
      List<DataFile> dataFiles,
      List<String> idColNames,
      Set<Integer> pkFieldIds) {
    Schema schema = table.schema();
    Schema pkSchema = TypeUtil.select(schema, Sets.newHashSet(pkFieldIds));

    // Read the PK tuples present in each data file (in offset order).
    List<Map<PkKey, Record>> pkRecordsPerFile = new ArrayList<>(dataFiles.size());
    for (DataFile dataFile : dataFiles) {
      pkRecordsPerFile.add(readPrimaryKeys(table, dataFile, pkSchema, idColNames));
    }

    // Determine which PK values appear in more than one file. The winner is the last file (newest
    // by control-topic offset) that contains the key; all earlier files holding that key are
    // "losers" whose rows must be removed via equality deletes.
    Map<PkKey, Integer> winningFileIdxByKey = new HashMap<>();
    for (int fileIdx = 0; fileIdx < pkRecordsPerFile.size(); fileIdx++) {
      for (PkKey key : pkRecordsPerFile.get(fileIdx).keySet()) {
        winningFileIdxByKey.put(key, fileIdx);
      }
    }

    // For each file, collect the PK records that are losers in that file (i.e. a later file holds
    // the same key). Group by the file's partition so equality deletes can be written per spec /
    // partition.
    Map<PartitionKey, List<Record>> losersByPartition = new LinkedHashMap<>();
    int loserCount = 0;
    for (int fileIdx = 0; fileIdx < dataFiles.size(); fileIdx++) {
      DataFile dataFile = dataFiles.get(fileIdx);
      Map<PkKey, Record> pkRecords = pkRecordsPerFile.get(fileIdx);
      for (Map.Entry<PkKey, Record> entry : pkRecords.entrySet()) {
        Integer winningFileIdx = winningFileIdxByKey.get(entry.getKey());
        if (winningFileIdx != null && winningFileIdx != fileIdx) {
          PartitionKey partitionKey = new PartitionKey(dataFile.specId(), dataFile.partition());
          losersByPartition
              .computeIfAbsent(partitionKey, k -> new ArrayList<>())
              .add(entry.getValue());
          loserCount++;
        }
      }
    }

    if (losersByPartition.isEmpty()) {
      // No cross-file duplicates: behave exactly like the append-only path.
      return new Result(dataFiles, Lists.newArrayList());
    }

    List<DeleteFile> deleteFiles =
        writeEqualityDeletes(table, pkSchema, pkFieldIds, losersByPartition);

    LOG.info(
        "Coordinator PK dedup for table {} during commit {}: removed {} duplicate row(s) across {} "
            + "file(s) using {}={}, wrote {} equality delete file(s)",
        tableIdentifier,
        commitId,
        loserCount,
        dataFiles.size(),
        ID_COLS_PROP,
        String.join(",", idColNames),
        deleteFiles.size());

    return new Result(dataFiles, deleteFiles);
  }

  private static Map<PkKey, Record> readPrimaryKeys(
      Table table, DataFile dataFile, Schema pkSchema, List<String> idColNames) {
    // Use a map keyed on the PK tuple so duplicates *within* a single file collapse to one record.
    Map<PkKey, Record> result = new LinkedHashMap<>();
    InputFile inputFile = table.io().newInputFile(dataFile.path().toString());
    FileFormat format = dataFile.format();

    CloseableIterable<Record> records;
    switch (format) {
      case PARQUET:
        records =
            Parquet.read(inputFile)
                .project(pkSchema)
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(pkSchema, fileSchema))
                .build();
        break;
      case ORC:
        records =
            ORC.read(inputFile)
                .project(pkSchema)
                .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(pkSchema, fileSchema))
                .build();
        break;
      case AVRO:
        records =
            Avro.read(inputFile)
                .project(pkSchema)
                .createReaderFunc(DataReader::create)
                .build();
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported file format for coordinator PK dedup: " + format);
    }

    try (CloseableIterable<Record> closeableRecords = records) {
      for (Record record : closeableRecords) {
        List<Object> values = new ArrayList<>(idColNames.size());
        for (String colName : idColNames) {
          values.add(record.getField(colName));
        }
        result.put(new PkKey(values), record);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read primary keys from data file " + dataFile.path(), e);
    }
    return result;
  }

  private static List<DeleteFile> writeEqualityDeletes(
      Table table,
      Schema pkSchema,
      Set<Integer> pkFieldIds,
      Map<PartitionKey, List<Record>> losersByPartition) {
    Map<Integer, PartitionSpec> specsById = table.specs();
    int[] equalityFieldIds = Ints.toArray(pkFieldIds);

    List<DeleteFile> deleteFiles = Lists.newArrayList();

    for (Map.Entry<PartitionKey, List<Record>> entry : losersByPartition.entrySet()) {
      PartitionKey partitionKey = entry.getKey();
      List<Record> losers = entry.getValue();
      PartitionSpec spec = specsById.get(partitionKey.specId);
      if (spec == null) {
        spec = table.spec();
      }

      FileFormat format = FileFormat.PARQUET;
      FileAppenderFactory<Record> appenderFactory =
          new GenericAppenderFactory(
                  table.schema(),
                  spec,
                  equalityFieldIds,
                  pkSchema,
                  null)
              .setAll(table.properties());

      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
              .defaultSpec(spec)
              .operationId(UUID.randomUUID().toString())
              .format(format)
              .build();

      StructLike partition = partitionKey.partition;
      OutputFile outputFile =
          spec.isUnpartitioned()
              ? fileFactory.newOutputFile().encryptingOutputFile()
              : fileFactory.newOutputFile(partition).encryptingOutputFile();

      EqualityDeleteWriter<Record> writer =
          appenderFactory.newEqDeleteWriter(
              org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput(
                  outputFile, org.apache.iceberg.encryption.EncryptionKeyMetadata.empty()),
              format,
              partition);

      try (EqualityDeleteWriter<Record> eqWriter = writer) {
        eqWriter.write(losers);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to write equality delete file", e);
      }

      deleteFiles.add(writer.toDeleteFile());
    }

    return deleteFiles;
  }

  /** A primary-key tuple usable as a map key with value-based equals/hashCode (null-safe). */
  private static final class PkKey {
    private final List<Object> values;

    PkKey(List<Object> values) {
      this.values = values;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PkKey pkKey = (PkKey) o;
      return Objects.equals(values, pkKey.values);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(values);
    }

    @Override
    public String toString() {
      return "PkKey" + values;
    }
  }

  /** Identifies the partition (spec id + partition tuple) a losing row belongs to. */
  private static final class PartitionKey {
    private final int specId;
    private final StructLike partition;

    PartitionKey(int specId, StructLike partition) {
      this.specId = specId;
      this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PartitionKey that = (PartitionKey) o;
      return specId == that.specId && Objects.equals(toStringKey(), that.toStringKey());
    }

    @Override
    public int hashCode() {
      return Objects.hash(specId, toStringKey());
    }

    private String toStringKey() {
      return partition == null ? "" : partition.toString();
    }
  }
}
