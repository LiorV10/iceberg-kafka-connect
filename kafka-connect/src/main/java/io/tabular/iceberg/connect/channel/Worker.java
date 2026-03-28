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
  // When non-null, records on flagged partitions are rerouted to this table.
  // Set when a flag is detected; cleared by onFlagProcessed().
  private TableIdentifier reroute;
  // Partitions that contained a flag record.  Their offsets are excluded from
  // sourceOffsets so the flag can be re-read after restart to restore the pause state.
  private final Set<Integer> flaggedPartitions;
  // When true, committable() will pause the flagged partitions via context.pause().
  private boolean pendingPause;
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
    this.reroute = null;
    this.flaggedPartitions = new HashSet<>();
    this.pendingPause = false;
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

    // Defer the actual context.pause() until committable() time so that the flagWriterResult is
    // guaranteed to be included in the Committable (and therefore sent to the Coordinator) before
    // the partition is paused.  If context.pause() were called earlier (e.g. in save()), Kafka
    // Connect might stop calling put() before committable() is ever invoked, leaving the flag
    // result stranded in the Worker and never forwarded to the Coordinator.
    if (pendingPause) {
      pauseFlaggedPartitions();
      pendingPause = false;
    }

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
    if (reroute == null || !reroute.equals(tableIdentifier)) {
      // This worker either did not detect a flag, or is rerouting for a different table.
      return;
    }

    LOG.debug("Flag-processed signal received for table {}, clearing reroute", tableIdentifier);

    reroute = null;
    flaggedPartitions.clear();
    resumeAssignment();
  }

  /**
   * Drains only the pending flag results, leaving normal write results and source offsets
   * untouched.  Also applies the deferred {@code context.pause()} if it hasn't been applied yet.
   * Returns {@code null} if there are no pending flags.
   *
   * <p>Flag partition offsets are NOT included (they were never added to {@code sourceOffsets})
   * so that the flag record can be re-read after a task restart, which restores the pause state.
   */
  @Override
  public Committable drainPendingFlagCommittable() {
    if (flagWriterResults.isEmpty()) {
      return null;
    }

    LOG.info("Draining {} pending flag result(s) for eager send", flagWriterResults.size());

    List<WriterResult> flags = Lists.newArrayList(flagWriterResults);
    flagWriterResults.clear();

    // Apply the deferred pause now — the flag results are about to be sent to the Coordinator,
    // so we can safely pause the flagged partitions.
    if (pendingPause) {
      pauseFlaggedPartitions();
      pendingPause = false;
    }

    // Return a Committable with empty offsets — flag partition offsets must NOT be committed
    // so the flag can be re-read after restart to restore the pause state.
    return new Committable(Maps.newHashMap(), flags);
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    reroute = null;
    flaggedPartitions.clear();
    pendingPause = false;
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      sinkRecords.forEach(this::save);
    }
  }

  private void save(SinkRecord record) {
    // Always check for flag records first, even when a reroute is active,
    // so that flags from other partitions are still captured.
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

      reroute = tableIdentifier;
      flaggedPartitions.add(record.kafkaPartition());
      pendingPause = true;

      if (context != null) {
        context.requestCommit();
      }

      // Do NOT update sourceOffsets for the flag record — its offset must not be committed
      // so that the flag is re-read after a task restart.  The pause state is restored by
      // re-reading the flag; the eager flag send in CommitterImpl ensures the flag result
      // reaches the Coordinator promptly even if START_COMMIT hasn't arrived yet.
      LOG.info(
          "Flag detected — rerouting same-batch records to {} (branch: {})",
          tableContext.tableIdentifier(),
          tableContext.branch());
      return;
    }

    // If a reroute is active and this record is on a flagged partition,
    // reroute it to the flag table but do NOT track its offset (so the
    // flag can be re-read after restart).
    if (reroute != null && flaggedPartitions.contains(record.kafkaPartition())) {
      writerForTable(reroute.toString(), record, true).write(record);
      return;
    }

    // Normal record processing — update offset tracking
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    sourceOffsets.put(
            new TopicPartition(record.topic(), record.kafkaPartition()),
            new Offset(record.kafkaOffset() + 1, record.timestamp()));

    if (config.dynamicTablesEnabled()) {
      routeRecordDynamically(record);
    } else {
      routeRecordStatically(record);
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

  private void pauseFlaggedPartitions() {
    if (context != null && !flaggedPartitions.isEmpty()) {
      TopicPartition[] partitions = context.assignment().stream()
              .filter(topicPartition -> flaggedPartitions.contains(topicPartition.partition()))
              .toArray(TopicPartition[]::new);

      context.pause(partitions);
      LOG.debug("Paused {} partition(s) for flagged partitions {}", partitions.length, flaggedPartitions);
    }
  }

  private void resumeAssignment() {
    if (context != null) {
      // Always call context.assignment() fresh for the same reason as pauseFlaggedPartitions().
      context.resume(context.assignment().toArray(new TopicPartition[0]));
    }
  }

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
