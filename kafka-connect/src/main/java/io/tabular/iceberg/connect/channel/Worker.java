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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.TableContext;
import io.tabular.iceberg.connect.data.FlagWriterResult;
import io.tabular.iceberg.connect.data.IcebergWriterFactory;
import io.tabular.iceberg.connect.data.Offset;
import io.tabular.iceberg.connect.data.RecordWriter;
import io.tabular.iceberg.connect.data.Utilities;
import io.tabular.iceberg.connect.data.WriterResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: rename to WriterImpl later, minimize changes for clearer commit history for now
class Worker implements Writer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final IcebergSinkConfig config;
  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writers;
  private final Map<TopicPartition, Offset> sourceOffsets;
  private final List<WriterResult> flagWriterResults;
  /**
   * Records that arrived after a flag in the same {@code put()} batch.  Their offsets are NOT
   * advanced until they are actually processed (when {@link #onFlagProcessed()} fires), so a
   * connector restart before processing would safely re-deliver them from Kafka.
   */
  private final List<SinkRecord> pendingAfterFlag;
  /**
   * The {@link SinkTaskContext} used to pause/resume source-topic partitions around a flag.
   * May be {@code null} in unit tests that use the package-private test constructor.
   */
  private final SinkTaskContext context;

  Worker(IcebergSinkConfig config, Catalog catalog, SinkTaskContext context) {
    this(config, new IcebergWriterFactory(catalog, config), context);
  }

  Worker(IcebergSinkConfig config, Catalog catalog) {
    this(config, catalog, null);
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
    this.pendingAfterFlag = Lists.newArrayList();
    this.context = context;
  }

  @Override
  public Committable committable() {
    List<WriterResult> writeResults =
        writers.values().stream().flatMap(writer -> writer.complete().stream()).collect(toList());

    // Add flag writer results to the list
    writeResults.addAll(flagWriterResults);

    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    // NOTE: do NOT clear pendingAfterFlag here.  Those records have not been written yet
    // and will be processed when onFlagProcessed() is called after the Coordinator confirms
    // the branch switch.  Their offsets are also not yet committed.

    return new Committable(offsets, writeResults);
  }

  /**
   * Called by {@link CommitterImpl} when the Coordinator broadcasts the flag-processed sentinel.
   * Resumes the paused source-topic partitions and then processes any records that were buffered
   * after the flag in the same batch.
   */
  @Override
  public void onFlagProcessed() {
    LOG.debug("Flag-processed signal received — resuming source partitions");
    resumeAssignment();
    if (!pendingAfterFlag.isEmpty()) {
      List<SinkRecord> pending = Lists.newArrayList(pendingAfterFlag);
      pendingAfterFlag.clear();
      // write() may encounter another flag in the buffered records; in that case it will
      // buffer the remainder again and wait for the next onFlagProcessed() signal.
      write(pending);
    }
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    pendingAfterFlag.clear();
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords == null || sinkRecords.isEmpty()) {
      return;
    }
    boolean flagFound = false;
    for (SinkRecord record : sinkRecords) {
      if (flagFound) {
        // Buffer records that arrive after the flag in this batch.  Their offsets are NOT
        // advanced here; they will be processed (and their offsets committed) only after
        // onFlagProcessed() fires, so a crash before that safely re-delivers them.
        pendingAfterFlag.add(record);
      } else {
        save(record);
        if (Utilities.isFlagRecord(record, this.config.flagKeyPrefix())) {
          flagFound = true;
        }
      }
    }
  }

  private void save(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    sourceOffsets.put(
        new TopicPartition(record.topic(), record.kafkaPartition()),
        new Offset(record.kafkaOffset() + 1, record.timestamp()));

    if (Utilities.isFlagRecord(record, this.config.flagKeyPrefix())) {
      LOG.info(
          "Flag record detected at topic: {}, partition: {}, offset: {}",
          record.topic(),
          record.kafkaPartition(),
          record.kafkaOffset());
      String tableName = extractRouteValue(record.value(), this.config.tablesRouteField());
      TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
      TableContext tableContext =
          TableContext.parse(tableIdentifier, this.config.branchesRegexDelimiter());

      String recordJson = serializeRecordToJson(record.value());
      FlagWriterResult flagResult =
          new FlagWriterResult(tableIdentifier, tableContext.branch(), recordJson);
      flagWriterResults.add(flagResult);

      // Pause all assigned source-topic partitions so that no further records are delivered
      // until the Coordinator confirms the branch switch via onFlagProcessed().
      LOG.info(
          "Flag detected — pausing source partitions until Coordinator processes the flag "
              + "(table: {}, branch: {})",
          tableContext.tableIdentifier(),
          tableContext.branch());
      pauseAssignment();
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
    Preconditions.checkNotNull(routeField, String.format("Route field cannot be null with dynamic routing at topic: %s, partition: %d, offset: %d", record.topic(), record.kafkaPartition(), record.kafkaOffset()));

    String routeValue = extractRouteValue(record.value(), routeField);
    if (routeValue != null) {
      String tableName = routeValue.toLowerCase();
      writerForTable(tableName, record, true).write(record);
    }
  }

  private String extractRouteValue(Object recordValue, String routeField) {
    return extractString(recordValue, routeField);
  }

  private String extractString(Object recordValue, String field) {
    if (recordValue == null) {
      return null;
    }

    Object value = Utilities.extractFromRecordValue(recordValue, field);
    return value == null ? null : value.toString();
  }

  private String serializeRecordToJson(Object recordValue) {
    try {
      if (recordValue instanceof Map) {
        return MAPPER.writeValueAsString(recordValue);
      } else if (recordValue instanceof Struct) {
        Struct struct = (Struct) recordValue;
        Map<String, Object> map = new LinkedHashMap<>();
        struct.schema().fields().forEach(field -> map.put(field.name(), struct.get(field)));
        return MAPPER.writeValueAsString(map);
      }
      return MAPPER.writeValueAsString(recordValue);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void pauseAssignment() {
    if (context != null) {
      // Always call context.assignment() fresh: the partition set can change on rebalance,
      // so caching it would risk pausing stale or missing newly assigned partitions.
      context.pause(context.assignment().toArray(new TopicPartition[0]));
    }
  }

  private void resumeAssignment() {
    if (context != null) {
      // Always call context.assignment() fresh for the same reason as pauseAssignment().
      context.resume(context.assignment().toArray(new TopicPartition[0]));
    }
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
