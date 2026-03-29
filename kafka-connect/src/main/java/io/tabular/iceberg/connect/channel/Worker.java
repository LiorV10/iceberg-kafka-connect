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
  private SinkRecord pausingFlag;
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
    this.isPaused = false;
    this.pausingFlag = null;
    this.context = context;
  }

  @Override
  public Committable committable() {
    LOG.debug("About to commit latest records");

    List<WriterResult> writeResults =
        writers.values().stream().flatMap(writer -> writer.complete().stream()).collect(toList());

    // Add flag writer results to the list
    writeResults.addAll(flagWriterResults);

    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    // NOTE: do NOT clear reroute here.  Reroute must remain set across commit cycles because the
    // flag result is sent in this cycle's sendCommitResponse(), and onFlagProcessed() is only
    // triggered later when the Coordinator broadcasts the per-table sentinel after all partitions
    // have reported — which may happen in a subsequent commit cycle.

    LOG.debug("Committing {} records", writeResults.size());

    return new Committable(offsets, writeResults);
  }

  /**
   * Called by {@link CommitterImpl} when it receives the per-table sentinel
   * {@link org.apache.iceberg.connect.events.CommitToTable} event that the Coordinator broadcasts
   * after it has collected flag-containing {@code DataWritten} events from <em>all</em> source
   * partitions for a specific table and executed the flag action (e.g. branch switch).
   * <p>
   * Only acts when this worker is currently rerouting to {@code tableIdentifier}.  Workers for
   * other tables, or workers that never detected a flag, are unaffected.
   */
  @Override
  public void onFlagProcessed(TableIdentifier tableIdentifier) {
    if (!this.isPaused) {
      // This worker did not detect a flag — it was never paused, nothing to do.
      return;
    }

    LOG.debug("Flag-processed signal received for table {}, clearing reroute", tableIdentifier);

    sourceOffsets.put(
            new TopicPartition(this.pausingFlag.topic(), this.pausingFlag.kafkaPartition()),
            new Offset(this.pausingFlag.kafkaOffset() + 1, this.pausingFlag.timestamp()));
    context.offset(new TopicPartition(this.pausingFlag.topic(), this.pausingFlag.kafkaPartition()), this.pausingFlag.kafkaOffset() + 1);

    this.pausingFlag = null;
    resumeAssignment();
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    this.isPaused = false;
    this.pausingFlag = null;
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

    if (Utilities.isFlagRecord(record, this.config.flagKeyPrefix())) {
      LOG.info(
          "Flag record detected at topic: {}, partition: {}, offset: {}",
          record.topic(),
          record.kafkaPartition(),
          record.kafkaOffset());

      sourceOffsets.put(
              new TopicPartition(record.topic(), record.kafkaPartition()),
              new Offset(record.kafkaOffset(), record.timestamp()));

      String tableName = extractRouteValue(record.value(), this.config.tablesRouteField());
      TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
      TableContext tableContext =
          TableContext.parse(tableIdentifier, this.config.branchesRegexDelimiter());

      String recordJson = serializeRecordToJson(record);
      FlagWriterResult flagResult =
          new FlagWriterResult(tableIdentifier, tableContext.branch(), recordJson);
      flagWriterResults.add(flagResult);

      this.pausingFlag = record;
      pauseAssignment(record.kafkaPartition());
      LOG.info(
          "Flag detected — rerouting same-batch records to {} (branch: {})",
          tableContext.tableIdentifier(),
          tableContext.branch());
    } else {
      // the consumer stores the offsets that corresponds to the next record to consume,
      // so increment the record offset by one
      sourceOffsets.put(
              new TopicPartition(record.topic(), record.kafkaPartition()),
              new Offset(record.kafkaOffset() + 1, record.timestamp()));

      LOG.debug("Processing record at {}", record.kafkaOffset());

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

      context.resume(partitions);
      context.pause(partitions);
      this.isPaused = true;
      LOG.debug("Context has paused for partition {} at topic {}", flagPartition, Arrays.stream(partitions).findFirst().get().topic());
    }
  }

  private void resumeAssignment() {
    if (context != null) {
      // Always call context.assignment() fresh for the same reason as pauseAssignment().
      this.isPaused = false;
      context.resume(context.assignment().toArray(new TopicPartition[0]));
    }
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
