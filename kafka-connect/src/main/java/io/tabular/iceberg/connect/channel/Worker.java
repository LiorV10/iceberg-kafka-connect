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

  // Non-null while a flag has been detected but not yet processed by the Coordinator.
  // Used to route post-flag records in the same batch to the flag's target table,
  // and to identify the sentinel when onFlagProcessed() is called.
  private TableIdentifier reroute;

  // The TopicPartition and offset of the detected flag record.  The offset is committed AT the
  // flag (not past it) so that if the worker crashes before the Coordinator sends the
  // FLAG_PROCESSED_SENTINEL, the next restart will re-read and re-process the flag record,
  // naturally re-establishing the paused state.
  private TopicPartition flagTopicPartition;
  private long flagOffset;
  private Long flagTimestamp;

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
    // NOTE: do NOT clear reroute/flagTopicPartition here.  They must survive across commit cycles
    // because the flag result is sent in this cycle's sendCommitResponse(), and onFlagProcessed()
    // is triggered only when the Coordinator broadcasts the per-table sentinel after all partitions
    // have reported — which may happen in a subsequent commit cycle.

    // Deferred pause: now that the flag batch is committed, pause the flag partition so that
    // no new records arrive from it until the Coordinator signals flag-processed.
    if (reroute != null && !isPaused) {
      pauseAssignment(flagTopicPartition.partition());
    }

    LOG.debug("Committing {} records", writeResults.size());

    return new Committable(offsets, writeResults);
  }

  /**
   * Called by {@link CommitterImpl} when it receives the per-table sentinel
   * {@link org.apache.iceberg.connect.events.CommitToTable} event that the Coordinator broadcasts
   * after it has collected flag-containing {@code DataWritten} events from <em>all</em> source
   * partitions for a specific table and executed the flag action (e.g. branch switch).
   *
   * <p>Only acts when this worker has a pending flag for exactly {@code tableIdentifier}. Workers
   * for other tables, or workers that never detected a flag, are unaffected.
   *
   * <p>On success, advances the committed source offset one past the flag record so that a
   * subsequent restart does not re-read the flag.
   */
  @Override
  public void onFlagProcessed(TableIdentifier tableIdentifier) {
    if (reroute == null) {
      // This worker did not detect a flag — nothing to do.
      return;
    }
    if (!reroute.equals(tableIdentifier)) {
      // Sentinel is for a different table — this worker is still waiting for its own.
      return;
    }

    LOG.debug("Flag-processed signal received for table {}, advancing offset and resuming", tableIdentifier);

    // Advance the committed source offset one past the flag record.  Storing it in sourceOffsets
    // here means it will be included in the next sendCommitResponse(), committing F+1 to the
    // control-group consumer-group.  After that, a restart will begin at F+1 and the flag will
    // not be re-read again.
    if (flagTopicPartition != null) {
      sourceOffsets.put(flagTopicPartition, new Offset(flagOffset + 1L, flagTimestamp));
    }

    // Clear flag state
    reroute = null;
    flagTopicPartition = null;

    resumeAssignment();
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    this.isPaused = false;
    this.reroute = null;
    this.flagTopicPartition = null;
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

    TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());

    if (Utilities.isFlagRecord(record, this.config.flagKeyPrefix())) {
      LOG.info(
          "Flag record detected at topic: {}, partition: {}, offset: {}",
          record.topic(),
          record.kafkaPartition(),
          record.kafkaOffset());

      // Commit AT the flag offset (not offset+1) so that if the worker crashes before the
      // Coordinator sends the FLAG_PROCESSED_SENTINEL, the consumer restarts from F and
      // re-reads the flag, naturally re-establishing the paused state.
      sourceOffsets.put(tp, new Offset(record.kafkaOffset(), record.timestamp()));
      this.flagTopicPartition = tp;
      this.flagOffset = record.kafkaOffset();
      this.flagTimestamp = record.timestamp();

      String tableName = extractRouteValue(record.value(), this.config.tablesRouteField());
      TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
      TableContext tableContext =
          TableContext.parse(tableIdentifier, this.config.branchesRegexDelimiter());

      String recordJson = serializeRecordToJson(record);
      FlagWriterResult flagResult =
          new FlagWriterResult(tableIdentifier, tableContext.branch(), recordJson);
      flagWriterResults.add(flagResult);

      this.reroute = tableIdentifier;

      // Request a commit so the FlagWriterResult is sent to the Coordinator promptly.
      if (context != null) {
        context.requestCommit();
      }

      LOG.info(
          "Flag detected — rerouting same-batch records to {} (branch: {})",
          tableContext.tableIdentifier(),
          tableContext.branch());
    } else {
      // For records on the flag partition that arrive after the flag in the same batch: do NOT
      // advance the committed offset past the flag.  This keeps the committed offset at F so that
      // a restart will re-read the flag and re-establish the paused state correctly.
      if (reroute == null || !tp.equals(flagTopicPartition)) {
        sourceOffsets.put(tp, new Offset(record.kafkaOffset() + 1, record.timestamp()));
      }

      if (reroute != null) {
        // Post-flag record in the same batch: route to the flag's target table.
        writerForTable(reroute.toString(), record, true).write(record);
      } else if (config.dynamicTablesEnabled()) {
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
    if (context != null) {
      TopicPartition[] partitions = context.assignment().stream()
              .filter(topicPartition -> flagPartition.equals(topicPartition.partition()))
              .toArray(TopicPartition[]::new);
      context.pause(partitions);
      LOG.debug("Paused partition {}", flagPartition);
    }
    this.isPaused = true;
  }

  private void resumeAssignment() {
    if (context != null) {
      context.resume(context.assignment().toArray(new TopicPartition[0]));
    }
    this.isPaused = false;
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
