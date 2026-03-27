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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.data.FlagWriterResult;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.RecordWriter;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.data.WriterResult;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: rename to WriterImpl later, minimize changes for clearer commit history for now
class Worker implements Writer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private final IcebergSinkConfig config;
  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writers;
  private final Map<TopicPartition, Offset> sourceOffsets;
  /** Pending flag WriterResults, cleared once included in a committable(). */
  private final List<WriterResult> flagWriterResults;
  /**
   * Maps each currently-paused source partition to the flag's {@link TableIdentifier}.
   * Survives across commit cycles (not cleared in committable()) until {@link
   * #onFlagProcessed(TableIdentifier)} removes it when the coordinator confirms the flag.
   */
  private final Map<TopicPartition, TableIdentifier> pausedPartitionTables;
  /** {@link SinkTaskContext} used to pause/resume source-topic partitions. */
  private final SinkTaskContext context;

  Worker(IcebergSinkConfig config, Catalog catalog, SinkTaskContext context) {
    this(config, new IcebergWriterFactory(catalog, config), context);
  }

  @VisibleForTesting
  Worker(IcebergSinkConfig config, IcebergWriterFactory writerFactory) {
    this(config, writerFactory, null);
  }

  @VisibleForTesting
  Worker(IcebergSinkConfig config, IcebergWriterFactory writerFactory, SinkTaskContext context) {
    this.config = config;
    this.writerFactory = writerFactory;
    this.writers = Maps.newHashMap();
    this.sourceOffsets = Maps.newHashMap();
    this.flagWriterResults = Lists.newArrayList();
    this.pausedPartitionTables = Maps.newLinkedHashMap();
    this.context = context;
  }

  @Override
  public Committable committable() {
    List<WriterResult> writeResults =
        writers.values().stream().flatMap(writer -> writer.complete().stream()).collect(toList());
    writeResults.addAll(flagWriterResults);

    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    // NOTE: pausedPartitionTables is NOT cleared here; it survives until onFlagProcessed().

    return new Committable(offsets, writeResults, Maps.newHashMap(pausedPartitionTables));
  }

  /**
   * Called by {@link CommitterImpl} when the coordinator broadcasts the flag-processed sentinel
   * for {@code tableIdentifier}.  Resumes the partitions that were paused for that table.
   */
  @Override
  public void onFlagProcessed(TableIdentifier tableIdentifier) {
    List<TopicPartition> toResume =
        pausedPartitionTables.entrySet().stream()
            .filter(e -> e.getValue().equals(tableIdentifier))
            .map(Map.Entry::getKey)
            .collect(toList());
    toResume.forEach(pausedPartitionTables::remove);
    if (!toResume.isEmpty() && context != null) {
      LOG.info(
          "Resuming {} partition(s) after flag processed for table {}",
          toResume.size(),
          tableIdentifier);
      context.resume(toResume.toArray(new TopicPartition[0]));
    }
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    pausedPartitionTables.clear();
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      sinkRecords.forEach(this::save);
    }
  }

  private void save(SinkRecord record) {
    TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
    // Always update sourceOffsets (including for flag records) so the offset past the flag is
    // committed to Kafka atomically with the flag DataWritten event in the same Kafka transaction.
    // This means the partition will NOT re-read the flag on crash; instead, the persistent
    // "PAUSED:<table>" metadata in the consumer-group offsets keeps the partition paused until
    // the coordinator broadcasts the flag-processed sentinel.
    sourceOffsets.put(tp, new Offset(record.kafkaOffset() + 1, record.timestamp()));

    if (Utilities.isFlagRecord(record, config.flagKeyPrefix())) {
      LOG.info(
          "Flag record detected at topic={}, partition={}, offset={}",
          record.topic(),
          record.kafkaPartition(),
          record.kafkaOffset());
      String tableName = extractRouteValue(record.value(), config.tablesRouteField());
      if (tableName == null) {
        LOG.warn(
            "Flag record at partition={}, offset={} has no route-field value; skipping pause",
            record.kafkaPartition(),
            record.kafkaOffset());
        return;
      }
      TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
      flagWriterResults.add(new FlagWriterResult(tableIdentifier));
      pausedPartitionTables.put(tp, tableIdentifier);
      if (context != null) {
        context.pause(tp);
      }
    } else {
      if (config.dynamicTablesEnabled()) {
        routeRecordDynamically(record);
      } else {
        routeRecordStatically(record);
      }
    }
  }

  private void routeRecordStatically(SinkRecord record) {
    String routeField = config.tablesRouteField();

    if (routeField == null) {
      // route to all tables
      config
          .tables()
          .forEach(
              tableName -> {
                writerForTable(tableName, record, false).write(record);
              });

    } else {
      String routeValue = extractRouteValue(record.value(), routeField);
      if (routeValue != null) {
        config
            .tables()
            .forEach(
                tableName ->
                    config
                        .tableConfig(tableName)
                        .routeRegex()
                        .ifPresent(
                            regex -> {
                              if (regex.matcher(routeValue).matches()) {
                                writerForTable(tableName, record, false).write(record);
                              }
                            }));
      }
    }
  }

  private void routeRecordDynamically(SinkRecord record) {
    String routeField = config.tablesRouteField();
    Preconditions.checkNotNull(
        routeField,
        String.format(
            "Route field cannot be null with dynamic routing at topic: %s, partition: %d, offset: %d",
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset()));

    String routeValue = extractRouteValue(record.value(), routeField);
    if (routeValue != null) {
      String tableName = routeValue.toLowerCase();
      writerForTable(tableName, record, true).write(record);
    }
  }

  private String extractRouteValue(Object recordValue, String routeField) {
    if (recordValue == null) {
      return null;
    }
    Object routeValue = Utilities.extractFromRecordValue(recordValue, routeField);
    return routeValue == null ? null : routeValue.toString();
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
