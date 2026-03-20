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
   * When a flag record is encountered, subsequent records in the same polled batch are rerouted
   * to this table name. Once {@link CommitterImpl} has sent the flag result to the Coordinator,
   * it calls {@link #onFlagProcessed()} which clears this field so that newly delivered records
   * (after partition resume) are routed normally.
   */
  private String reroute;
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
    this.reroute = null;
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
    // NOTE: do NOT clear reroute here.  The partition is paused while reroute is active,
    // so no new records will arrive until onFlagProcessed() resumes it.  But reroute must
    // remain set across commit cycles because the flag result is sent in this cycle's
    // sendCommitResponse(), and onFlagProcessed() is only triggered later when the Coordinator
    // broadcasts the sentinel CommitComplete after all partitions have reported — which may
    // happen in a subsequent commit cycle.

    return new Committable(offsets, writeResults);
  }

  /**
   * Called by {@link CommitterImpl} when it receives the sentinel {@link
   * org.apache.iceberg.connect.events.CommitComplete} event that the Coordinator broadcasts after
   * it has collected flag-containing {@code DataWritten} events from <em>all</em> source
   * partitions and executed the flag action (e.g. branch switch).  Every worker receives this
   * event, but only the worker that actually paused on a flag (i.e. whose {@code reroute} field
   * is non-null) needs to act on it.
   * <p>
   * Guards against spurious calls: if {@code reroute} is already null this worker did not detect
   * a flag and has nothing to do.
   */
  @Override
  public void onFlagProcessed() {
    if (this.reroute == null) {
      // This worker did not detect a flag — it was never paused, nothing to do.
      return;
    }
    LOG.debug("Flag-processed signal received, clearing reroute for table {}", this.reroute);
    this.reroute = null;
    resumeAssignment();
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    this.reroute = null;
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      sinkRecords.forEach(this::save);
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

      // Reroute any remaining same-batch records to the flag branch, and pause so that
      // Kafka stops delivering new records until onFlagProcessed() resumes the partition.
      this.reroute = tableName;
      LOG.info(
          "Flag detected — pausing source partitions and rerouting same-batch records to {} "
              + "(branch: {})",
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

    if (this.reroute != null) {
      LOG.debug("Rerouting record to {}", this.reroute);
      writerForTable(this.reroute, record, true).write(record);
      return;
    }

    String routeValue = extractRouteValue(record.value(), routeField);
    if (routeValue != null) {
      writerForTable(routeValue.toLowerCase(), record, true).write(record);
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
