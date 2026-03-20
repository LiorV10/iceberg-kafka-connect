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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: rename to WriterImpl later, minimize changes for clearer commit history for now
class Worker implements Writer, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  /** Internal JSON field used to distinguish multiple occurrences of the same flag type. */
  static final String FLAG_SEQNO_FIELD = "__seqno__";
  private final IcebergSinkConfig config;
  private final IcebergWriterFactory writerFactory;
  private final Map<String, RecordWriter> writers;
  private final Map<TopicPartition, Offset> sourceOffsets;
  /**
   * Ordered list of writer results interleaved with flag results. Data results that were
   * accumulated before a flag are flushed at the flag boundary so the coordinator can commit
   * each data segment and then process its trailing flag in sequence:
   * [data1, flag1, data2, flag2, ...]
   */
  private final List<WriterResult> orderedResults;
  /**
   * Monotonically-increasing sequence number per flag type within a single commit cycle.
   * Used to distinguish two occurrences of the same flag type (e.g. two "END-LOAD" flags)
   * so the coordinator votes on them independently rather than deduplicating them.
   */
  private final Map<String, Integer> flagSequenceNumbers;
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
    this.orderedResults = Lists.newArrayList();
    this.flagSequenceNumbers = Maps.newHashMap();
    this.reroute = null;
  }

  @Override
  public Committable committable() {
    // Flush remaining writers (the final data segment with no trailing flag).
    List<WriterResult> remaining =
        writers.values().stream().flatMap(writer -> writer.complete().stream()).collect(toList());
    orderedResults.addAll(remaining);
    writers.clear();

    List<WriterResult> results = Lists.newArrayList(orderedResults);
    Map<TopicPartition, Offset> offsets = Maps.newHashMap(sourceOffsets);

    sourceOffsets.clear();
    orderedResults.clear();
    flagSequenceNumbers.clear();
    this.reroute = null;

    return new Committable(offsets, results);
  }

  @Override
  public void close() throws IOException {
    writers.values().forEach(RecordWriter::close);
    writers.clear();
    sourceOffsets.clear();
    orderedResults.clear();
    flagSequenceNumbers.clear();
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
       // Flag records are tracked in offsets but not written to data files
       // They will be sent to coordinator during commit to trigger branch switching
       LOG.info("Flag record detected at topic: {}, partition: {}, offset: {}",
                record.topic(), record.kafkaPartition(), record.kafkaOffset());
       String tableName = extractRouteValue(record.value(), this.config.tablesRouteField());
       TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
       TableContext context = TableContext.parse(tableIdentifier, this.config.branchesRegexDelimiter());

       // --- segment boundary: flush all in-flight writers so data rows that arrived BEFORE
       // this flag are committed as their own segment. Rows arriving AFTER this flag will
       // create fresh writers and form the next segment.
       List<WriterResult> preSegmentResults =
           writers.values().stream()
               .flatMap(writer -> writer.complete().stream())
               .collect(toList());
       writers.clear();
       orderedResults.addAll(preSegmentResults);

       // Assign a monotonically-increasing sequence number so the coordinator can vote on
       // each occurrence independently (e.g. two "END-LOAD" flags → seqno 1 and 2).
       // The counter is scoped to (flagType, sourcePartition) so that broadcast copies of
       // the same logical flag arriving from different Kafka partitions within the same task
       // all receive the same seqno.  Without per-partition tracking, the first broadcast copy
       // would get seqno=1 and the second seqno=2, causing the Coordinator to accumulate votes
       // in separate buckets and never reach quorum for either.
       String flagTypeField = this.config.flagTypeField();
       String flagType = flagTypeField != null ? extractString(record.value(), flagTypeField) : null;
       String perPartitionKey = (flagType != null ? flagType : "") + "|" + record.kafkaPartition();
       int seqno = flagSequenceNumbers.merge(perPartitionKey, 1, Integer::sum);

       // Embed the seqno in the serialised record so it survives the Kafka round-trip.
       String recordJson = serializeRecordToJsonWithSeqno(record.value(), seqno);
       FlagWriterResult flagResult = new FlagWriterResult(tableIdentifier, context.branch(), recordJson);
       orderedResults.add(flagResult);

       // All records after flag should be re-routed to flag's branch
       this.reroute = tableName;
       
       LOG.debug("Flag message queued for table: {}, branch: {}, seqno: {}",
           context.tableIdentifier(), context.branch(), seqno);
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

  private String serializeRecordToJsonWithSeqno(Object recordValue, int seqno) {
    try {
      Map<String, Object> map;
      if (recordValue instanceof Map) {
        map = new LinkedHashMap<>((Map<?, ?>) recordValue).entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue,
                (a, b) -> a, LinkedHashMap::new));
      } else if (recordValue instanceof Struct) {
        Struct struct = (Struct) recordValue;
        map = new LinkedHashMap<>();
        struct.schema().fields().forEach(field -> map.put(field.name(), struct.get(field)));
      } else {
        map = new LinkedHashMap<>();
        map.put("__raw__", recordValue);
      }
      map.put(FLAG_SEQNO_FIELD, seqno);
      return MAPPER.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
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
