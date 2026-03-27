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
import java.util.*;
import java.util.stream.Collectors;

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
  private boolean isPaused;
  private final SinkTaskContext context;

  // Non-null while this worker is paused waiting for coordinator to process a flag.
  // Used only by onFlagProcessed() to verify the sentinel is for the correct table.
  private TableIdentifier pendingFlagTable;

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
    this.isPaused = false;
    this.context = context;
    this.pendingFlagTable = null;
  }

  @Override
  public Committable committable() {
    LOG.debug("About to commit latest records");

    List<WriterResult> writeResults =
        writers.values().stream().flatMap(writer -> writer.complete().stream()).collect(toList());

    // Include pending flag results in every committable until the coordinator confirms flag
    // processing by calling onFlagProcessed().  Do NOT clear flagWriterResults here: if the
    // coordinator fails to process the flag in this commit cycle (e.g. the commit times out
    // before receiving all worker responses, or an exception prevents the sentinel from being
    // sent), the FlagWriterResult must survive to the next committable() call so that the flag
    // is retried on the following StartCommit.  flagWriterResults is cleared in
    // onFlagProcessed() (sentinel received) and in close() (task shutdown).
    writeResults.addAll(flagWriterResults);

    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    writers.clear();
    sourceOffsets.clear();
    // NOTE: flagWriterResults intentionally not cleared here — see above.

    LOG.debug("Committing {} records", writeResults.size());

    return new Committable(offsets, writeResults);
  }

  /**
   * Returns {@code true} while this worker is paused waiting for the Coordinator's
   * per-table sentinel {@link org.apache.iceberg.connect.events.CommitToTable} event after
   * detecting a flag record.  {@link CommitterImpl} uses this to switch to a positive
   * {@code poll} duration so the control-topic consumer actively waits for the
   * {@link org.apache.iceberg.connect.events.StartCommit} rather than returning immediately
   * with zero records.
   */
  @Override
  public boolean isPendingFlagCommit() {
    return isPaused;
  }

  @Override
  public void onFlagProcessed(TableIdentifier tableIdentifier) {
    if (pendingFlagTable == null || !pendingFlagTable.equals(tableIdentifier)) {
      // Either no flag was detected on this worker, or the sentinel is for a different table.
      return;
    }

    LOG.debug("Flag-processed signal received for table {}, resuming partitions", tableIdentifier);

    flagWriterResults.clear();
    resumeAssignment();
    pendingFlagTable = null;
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    this.isPaused = false;
    this.pendingFlagTable = null;
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      sinkRecords.forEach(this::save);
    }
  }

  private void save(SinkRecord record) {
    if (this.isPaused) {
      LOG.debug("Currently in pause, will process {} [topic: {}, partition: {}] when resume",
              record.kafkaOffset(), record.topic(), record.kafkaPartition());

      return;
    }

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

      String recordJson = serializeRecordToJson(record);
      FlagWriterResult flagResult =
          new FlagWriterResult(tableIdentifier, tableContext.branch(), recordJson);
      flagWriterResults.add(flagResult);
      this.pendingFlagTable = tableIdentifier;

      if (context != null) {
        context.requestCommit();
      }

      // Pause immediately so no further records from this partition are delivered until the
      // coordinator has processed the flag and sent the per-table sentinel.
      pauseAssignment(record.kafkaPartition());
      LOG.info(
          "Flag detected at partition {} offset {} — pausing partition for table {}",
          record.kafkaPartition(),
          record.kafkaOffset(),
          tableContext.tableIdentifier());
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

  private String serializeRecordToJson(SinkRecord record) {
    try {
      Map<String, Object> envelope = new LinkedHashMap<>();
      envelope.put("topic", record.topic());
      envelope.put("partition", record.kafkaPartition());
      envelope.put("offset", record.kafkaOffset());
      envelope.put("timestamp", record.timestamp());
      envelope.put("key", record.key() != null ? record.key().toString() : null);
      envelope.put("value", flattenValue(record.value()));
      return MAPPER.writeValueAsString(envelope);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private Object flattenValue(Object recordValue) {
    if (recordValue instanceof Struct) {
      Struct struct = (Struct) recordValue;
      Map<String, Object> map = new LinkedHashMap<>();
      struct.schema().fields().forEach(field -> map.put(field.name(), struct.get(field)));
      return map;
    } else if (recordValue instanceof Map) {
      return new LinkedHashMap<>((Map<String, Object>) recordValue);
    }
    return recordValue;
  }

  private void pauseAssignment(Integer flagPartition) {
    LOG.debug("About to pause task, context is {}", context);
    if (context != null) {
      TopicPartition[] partitions = context.assignment().stream()
              .filter(topicPartition -> flagPartition.equals(topicPartition.partition()))
              .toArray(TopicPartition[]::new);

      context.pause(partitions);
      this.isPaused = true;
      LOG.debug("Context has paused partition {}", flagPartition);
    }
  }

  private void resumeAssignment() {
    if (context != null) {
      // Always call context.assignment() fresh for the same reason as pauseAssignment().
      context.resume(context.assignment().toArray(new TopicPartition[0]));
      this.isPaused = false;
    }
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
