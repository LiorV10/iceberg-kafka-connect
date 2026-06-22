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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs <strong>true row-level</strong> primary-key deduplication across the data files of a
 * single commit batch.
 *
 * <p>Background: when the catalog (e.g. a REST catalog pod) is unstable a commit can fail
 * <em>after</em> a worker has durably written its data files but <em>before</em> the source-topic
 * offsets for that worker have advanced. Kafka Connect then restarts the task, rewinds to the last
 * committed offsets and re-processes the same source records, writing them into brand-new data
 * files (new paths). The path-based {@link Deduplicated} cannot recognize these as duplicates, so
 * the same primary key ends up in two data files within one snapshot. Equality deletes do not help
 * because they never apply to data files written in the same snapshot.
 *
 * <p>This helper reads the configured identifier (primary key) column(s) and the {@code _pos} row
 * position from every data file in the batch, then for any PK that occurs in more than one row keeps
 * the occurrence carried by the data file reported on the highest control-topic offset (the most
 * recently written copy) and emits a {@link PositionDelete} for every other occurrence. Position
 * deletes target an exact {@code (path, pos)} and therefore correctly remove the stale row while
 * sparing the survivor even though both live in the same snapshot.
 *
 * <p>Unlike a bounds-based heuristic this is exact and works for data files that contain many
 * primary keys, at the cost of reading the identifier columns of the just-written files in the
 * coordinator.
 */
class RowLevelDeduplicated {

  private static final Logger LOG = LoggerFactory.getLogger(RowLevelDeduplicated.class);

  private RowLevelDeduplicated() {}

  /**
   * Builds position-delete files that remove duplicate primary keys across the given data files.
   *
   * @param dataFiles data files about to be committed (already deduplicated by path)
   * @param fileOffsets control-topic offset of the envelope that reported each data file, keyed by
   *     data-file path; the highest offset wins when a PK collides
   * @param schema the table schema
   * @param spec the table partition spec, used to build the position-delete writer
   * @param identifierFieldIds the identifier (primary key) field ids; empty makes this a no-op
   * @param io the table {@link FileIO} used to read data files and write delete files
   * @param fileFactory factory for naming the output position-delete file(s)
   * @return the position-delete files to add to the commit, or an empty list when nothing collides
   */
  static List<DeleteFile> positionDeletes(
      List<DataFile> dataFiles,
      Map<String, Long> fileOffsets,
      Schema schema,
      PartitionSpec spec,
      Set<Integer> identifierFieldIds,
      FileIO io,
      OutputFileFactory fileFactory) {
    if (dataFiles.size() < 2 || identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      return Lists.newArrayList();
    }

    List<Integer> sortedIds = Lists.newArrayList(identifierFieldIds);
    sortedIds.sort(Integer::compareTo);

    Schema keySchema = TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds));
    // projection = identifier columns + _pos so we can build position deletes
    Schema projection =
        TypeUtil.join(keySchema, new Schema(MetadataColumns.ROW_POSITION));

    // key -> the occurrence we keep, plus the list of occurrences we must delete
    Map<List<Object>, Occurrence> winners = Maps.newHashMap();
    List<Occurrence> losers = Lists.newArrayList();

    for (DataFile dataFile : dataFiles) {
      long fileOffset = fileOffsets.getOrDefault(dataFile.path().toString(), Long.MIN_VALUE);
      try (CloseableIterable<Record> records = openFile(dataFile, projection, schema, io)) {
        for (Record record : records) {
          long pos = (Long) record.getField(MetadataColumns.ROW_POSITION.name());
          List<Object> key = keyOf(record, sortedIds, schema);

          Occurrence current = new Occurrence(dataFile.path().toString(), pos, fileOffset);
          Occurrence previous = winners.get(key);
          if (previous == null) {
            winners.put(key, current);
          } else if (current.offset > previous.offset) {
            // current is newer: previous becomes a loser, current wins
            losers.add(previous);
            winners.put(key, current);
          } else {
            // previous is newer (or equal): current is a loser
            losers.add(current);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to read data file for PK dedup: " + dataFile.path(), e);
      }
    }

    if (losers.isEmpty()) {
      return Lists.newArrayList();
    }

    LOG.warn(
        "Row-level PK dedup found {} duplicate row(s) across {} data files; emitting position "
            + "deletes for the stale occurrences",
        losers.size(),
        dataFiles.size());

    return writePositionDeletes(losers, spec, io, fileFactory);
  }

  private static List<Object> keyOf(Record record, List<Integer> sortedIds, Schema schema) {
    List<Object> key = Lists.newArrayListWithCapacity(sortedIds.size());
    for (Integer fieldId : sortedIds) {
      Types.NestedField field = schema.findField(fieldId);
      key.add(record.getField(field.name()));
    }
    return key;
  }

  private static List<DeleteFile> writePositionDeletes(
      List<Occurrence> losers, PartitionSpec spec, FileIO io, OutputFileFactory fileFactory) {
    // The connector's data files are written against the unpartitioned/default spec from the
    // worker, and the coordinator commits them with their own specId. Position deletes here are
    // emitted against the table's current spec with a null partition (unpartitioned), which is
    // valid because deletes reference files by path.
    OutputFile outputFile = fileFactory.newOutputFile().encryptingOutputFile();

    PositionDeleteWriter<Record> writer;
    try {
      FileFormat format = FileFormat.fromFileName(outputFile.location());
      FileFormat resolved = format == null ? FileFormat.PARQUET : format;
      writer =
          newPositionDeleteWriter(resolved, outputFile, spec, io);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create position delete writer", e);
    }

    PositionDelete<Record> posDelete = PositionDelete.create();
    try (PositionDeleteWriter<Record> closeableWriter = writer) {
      // sort by path then pos for well-formed position delete files
      losers.sort(
          (a, b) -> {
            int c = a.path.compareTo(b.path);
            return c != 0 ? c : Long.compare(a.pos, b.pos);
          });
      for (Occurrence loser : losers) {
        closeableWriter.write(posDelete.set(loser.path, loser.pos, null));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write position deletes", e);
    }

    return Lists.newArrayList(writer.toDeleteFile());
  }

  private static PositionDeleteWriter<Record> newPositionDeleteWriter(
      FileFormat format, OutputFile outputFile, PartitionSpec spec, FileIO io) throws IOException {
    Schema posDeleteSchema = DeleteSchemaUtil.pathPosSchema();
    switch (format) {
      case PARQUET:
        return Parquet.writeDeletes(outputFile)
            .forTable(null)
            .rowSchema(null)
            .withSpec(spec)
            .createWriterFunc(GenericParquetWriterShim::create)
            .overwrite()
            .buildPositionWriter();
      default:
        throw new UnsupportedOperationException(
            "Position delete format not supported for PK dedup: " + format);
    }
  }

  private static CloseableIterable<Record> openFile(
      DataFile dataFile, Schema projection, Schema tableSchema, FileIO io) {
    InputFile input = io.newInputFile(dataFile.path().toString());
    switch (dataFile.format()) {
      case PARQUET:
        return Parquet.read(input)
            .project(projection)
            .createReaderFunc(
                fileSchema ->
                    GenericParquetReaders.buildReader(projection, fileSchema, Maps.newHashMap()))
            .build();
      case AVRO:
        return Avro.read(input)
            .project(projection)
            .createReaderFunc(
                avroSchema -> DataReader.create(projection, avroSchema, Maps.newHashMap()))
            .build();
      case ORC:
        Schema projectionWithoutMeta =
            TypeUtil.selectNot(projection, MetadataColumns.metadataFieldIds());
        return ORC.read(input)
            .project(projectionWithoutMeta)
            .createReaderFunc(
                fileSchema ->
                    GenericOrcReader.buildReader(projection, fileSchema, Maps.newHashMap()))
            .build();
      default:
        throw new UnsupportedOperationException(
            "Cannot read data file for PK dedup: " + dataFile.format() + " " + dataFile.path());
    }
  }

  private static final class Occurrence {
    private final String path;
    private final long pos;
    private final long offset;

    Occurrence(String path, long pos, long offset) {
      this.path = path;
      this.pos = pos;
      this.offset = offset;
    }
  }
}
