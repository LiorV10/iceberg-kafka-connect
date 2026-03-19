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
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
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
  // Flags that have been fully activated (all partitions in their batch were processed).
  private final List<WriterResult> flagWriterResults;
  // Flags detected in the current write() call, waiting for all other partition records
  // in the same batch to finish before activation (barrier pattern).
  private final List<WriterResult> pendingFlagWriterResults;
  // Reroute destination queued by the current batch's flag, applied at the start of
  // the next write() call so records from other partitions in the same batch are
  // unaffected.
  private String pendingReroute;
  private String reroute;

  Worker(IcebergSinkConfig config, Catalog catalog) {
    this(config, new IcebergWriterFactory(catalog, config));
  }

  @VisibleForTesting
  Worker(IcebergSinkConfig config, IcebergWriterFactory writerFactory) {
    this.config = config;
    this.writerFactory = writerFactory;
    this.writers = Maps.newHashMap();
    this.sourceOffsets = Maps.newHashMap();
    this.flagWriterResults = Lists.newArrayList();
    this.pendingFlagWriterResults = Lists.newArrayList();
    this.pendingReroute = null;
    this.reroute = null;
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
    this.reroute = null;

    return new Committable(offsets, writeResults);
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
    flagWriterResults.clear();
    pendingFlagWriterResults.clear();
    this.pendingReroute = null;
    this.reroute = null;
  }

  @Override
  public void write(Collection<SinkRecord> sinkRecords) {
    // Activate any flags that were buffered in the previous write() call.
    // By this point every partition that was present in that earlier batch has
    // already been processed, satisfying the cross-partition ordering guarantee.
    activatePendingFlags();

    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      sinkRecords.forEach(this::save);
    }
  }

  /**
   * Promotes flags buffered from the previous write() batch into the active
   * {@code flagWriterResults} list and applies the pending reroute destination.
   * This is the barrier point: all records from ALL partitions in the previous
   * batch have been processed before this method runs.
   */
  private void activatePendingFlags() {
    if (!pendingFlagWriterResults.isEmpty()) {
      flagWriterResults.addAll(pendingFlagWriterResults);
      pendingFlagWriterResults.clear();
      LOG.debug("Activated {} pending flag(s) after all partitions in previous batch were processed",
          flagWriterResults.size());
    }
    if (pendingReroute != null) {
      this.reroute = pendingReroute;
      this.pendingReroute = null;
    }
  }

  private void save(SinkRecord record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    sourceOffsets.put(
        new TopicPartition(record.topic(), record.kafkaPartition()),
        new Offset(record.kafkaOffset() + 1, record.timestamp()));

    if (Utilities.isFlagRecord(record, this.config.flagKeyPrefix())) {
      int expectedPartition = this.config.flagSourcePartition();
      if (expectedPartition != -1 && record.kafkaPartition() != expectedPartition) {
        LOG.warn(
            "Ignoring flag-like record at topic: {}, partition: {}, offset: {} — "
                + "it arrived on partition {} but iceberg.flags.source-partition is set to {}. "
                + "Flag records must be produced to partition {} to guarantee ordering.",
            record.topic(), record.kafkaPartition(), record.kafkaOffset(),
            record.kafkaPartition(), expectedPartition, expectedPartition);
        // Treat as a regular data record so ordering invariant is preserved
        if (config.dynamicTablesEnabled()) {
          routeRecordDynamically(record);
        } else {
          routeRecordStatically(record);
        }
        return;
      }

      // Flag records are tracked in offsets but not written to data files.
      // They are staged as PENDING and will be activated at the START of the next
      // write() call, after all other partitions in this batch have been processed.
      LOG.info("Flag record detected at topic: {}, partition: {}, offset: {}. "
               + "Staging as pending until all partitions in this batch are processed.",
               record.topic(), record.kafkaPartition(), record.kafkaOffset());
      String tableName = extractRouteValue(record.value(), this.config.tablesRouteField());
      TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
      String branchesRegexDelimiter = this.config.branchesRegexDelimiter();
      TableContext context = branchesRegexDelimiter != null
          ? TableContext.parse(tableIdentifier, branchesRegexDelimiter)
          : new TableContext(tableIdentifier, null);

      String recordJson = serializeRecordToJson(record.value());
      FlagWriterResult flagResult = new FlagWriterResult(tableIdentifier, context.branch(), recordJson);
      pendingFlagWriterResults.add(flagResult);

      // Queue the reroute; it will be applied at the start of the NEXT write() call.
      this.pendingReroute = tableName;

      LOG.debug("Flag message staged for table: {}, branch: {}", context.tableIdentifier(), context.branch());
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

      if (this.reroute != null) {
        tableName = this.reroute;
        LOG.debug("Rerouting records from {} to {}", routeValue, tableName);
      }

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

  private RecordWriter writerForTable(
      String tableName, SinkRecord sample, boolean ignoreMissingTable) {
    return writers.computeIfAbsent(
        tableName, notUsed -> writerFactory.createWriter(tableName, sample, ignoreMissingTable));
  }
}
