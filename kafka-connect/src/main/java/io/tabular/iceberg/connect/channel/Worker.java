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
import java.util.Collections;
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
  private boolean isPaused;
  private final SinkTaskContext context;

  // Non-null while a flag has been detected but not yet cleared by onFlagProcessed().
  // Identifies the target table so onFlagProcessed() can match the correct sentinel.
  private TableIdentifier reroute;

  // The source TopicPartition that carried the flag record, and the committed offset for that
  // partition (F+1 — one past the flag).  Retained until onFlagProcessed() so it can re-insert
  // the offset WITHOUT FLAG_PENDING metadata, which permanently clears the persistent state.
  private TopicPartition flagTopicPartition;
  private Long flagCommittedOffset;

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

    LOG.debug("Committing {} records", writeResults.size());

    return new Committable(offsets, writeResults);
  }

  /**
   * Returns the per-partition FLAG_PENDING metadata to encode in the committed consumer-group
   * offsets while a flag is in progress.  When the flag partition is paused and waiting for the
   * Coordinator's sentinel, this returns {@code {flagPartition: "FLAG_PENDING:<rawTableId>"}}.
   * Otherwise returns an empty map.  {@link CommitterImpl} calls this inside
   * {@code sendCommitResponse()} so the metadata is committed atomically with the offset.
   */
  @Override
  public Map<TopicPartition, String> pendingFlagMetadata() {
    if (reroute == null || flagTopicPartition == null) {
      return Collections.emptyMap();
    }
    return Collections.singletonMap(
        flagTopicPartition, CommittableSupplier.FLAG_METADATA_PREFIX + reroute.toString());
  }

  /**
   * Called by {@link CommitterImpl} on startup when it detects {@link
   * CommittableSupplier#FLAG_METADATA_PREFIX} in a committed consumer-group offset.  Immediately
   * pauses the given partition and re-queues a {@link FlagWriterResult} for re-submission to the
   * Coordinator, without needing to re-read the flag record from the source topic.
   *
   * <p>The worker will remain paused until the Coordinator sends the FLAG_PROCESSED_SENTINEL and
   * {@link #onFlagProcessed} is called.
   */
  @Override
  public void restorePendingFlagState(
      TopicPartition tp, TableIdentifier tableId, long committedOffset) {
    LOG.info(
        "Restoring FLAG_PENDING state for partition {} table {} from committed offset {}",
        tp, tableId, committedOffset);

    // Reconstruct a minimal flag JSON.  The Coordinator only needs the "partition" field to count
    // votes; all other fields are informational.  The original flag offset is committedOffset-1
    // (we committed F+1 = past the flag).
    Map<String, Object> envelope = new LinkedHashMap<>();
    envelope.put("topic", tp.topic());
    envelope.put("partition", tp.partition());
    envelope.put("offset", committedOffset - 1L);
    envelope.put("timestamp", null);
    envelope.put("key", null);
    envelope.put("value", null);
    String recordJson;
    try {
      recordJson = MAPPER.writeValueAsString(envelope);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }

    TableContext tableContext = TableContext.parse(tableId, config.branchesRegexDelimiter());
    FlagWriterResult flagResult =
        new FlagWriterResult(tableId, tableContext.branch(), recordJson);
    flagWriterResults.add(flagResult);

    this.reroute = tableId;
    this.flagTopicPartition = tp;
    this.flagCommittedOffset = committedOffset;

    // Put the committed offset (F+1) into sourceOffsets so it is included in the next
    // sendCommitResponse().  This keeps the FLAG_PENDING metadata alive in controlGroupId until
    // onFlagProcessed() explicitly clears it.
    sourceOffsets.put(tp, new Offset(committedOffset, null));

    pauseAssignment(tp.partition());

    // Request a commit immediately so that the re-queued FlagWriterResult is delivered to the
    // Coordinator on the next poll cycle — exactly as it would be if the flag had been re-sent.
    // Without this, the restored state waits silently for the Coordinator's next periodic
    // START_COMMIT (which can be minutes away).
    if (context != null) {
      context.requestCommit();
    }

    LOG.info(
        "Restored FLAG_PENDING state for partition {} table {} — requested immediate commit",
        tp, tableId);
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
   * <p>Puts the committed offset (F+1) back into {@code sourceOffsets} WITHOUT
   * {@link CommittableSupplier#FLAG_METADATA_PREFIX} metadata so that the next
   * {@code sendCommitResponse()} permanently clears the persistent state in {@code controlGroupId}.
   */
  @Override
  public void onFlagProcessed(TableIdentifier tableIdentifier) {
    if (reroute == null || !reroute.equals(tableIdentifier)) {
      return;
    }

    LOG.debug("Flag-processed signal received for table {}, resuming", tableIdentifier);

    // Re-insert the committed offset WITHOUT FLAG_PENDING metadata.  Since reroute is about to be
    // set to null, pendingFlagMetadata() will return empty on the next commit cycle, causing
    // controlGroupId to be updated with plain F+1 — permanently clearing the persistent state.
    if (flagTopicPartition != null && flagCommittedOffset != null) {
      sourceOffsets.put(flagTopicPartition, new Offset(flagCommittedOffset, null));
    }

    // Clear flag state
    reroute = null;
    flagTopicPartition = null;
    flagCommittedOffset = null;

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
    this.flagCommittedOffset = null;
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

      // Commit F+1 (past the flag) as normal.  Crash-recovery persistence is handled by
      // encoding FLAG_PENDING:<tableId> in the OffsetAndMetadata.metadata() field (via
      // pendingFlagMetadata()), not by anchoring the offset at F.
      long committedOffset = record.kafkaOffset() + 1L;
      sourceOffsets.put(tp, new Offset(committedOffset, record.timestamp()));
      this.flagTopicPartition = tp;
      this.flagCommittedOffset = committedOffset;

      String tableName = extractRouteValue(record.value(), this.config.tablesRouteField());
      TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
      TableContext tableContext =
          TableContext.parse(tableIdentifier, this.config.branchesRegexDelimiter());

      String recordJson = serializeRecordToJson(record);
      FlagWriterResult flagResult =
          new FlagWriterResult(tableIdentifier, tableContext.branch(), recordJson);
      flagWriterResults.add(flagResult);

      this.reroute = tableIdentifier;

      // Pause immediately so that records after the flag (in the same poll batch or future batches)
      // are not processed until the Coordinator completes the flag action and sends the sentinel.
      pauseAssignment(tp.partition());

      // Request a commit so the FlagWriterResult is sent to the Coordinator promptly.
      if (context != null) {
        context.requestCommit();
      }

      LOG.info(
          "Flag detected — paused partition {}, queued FlagWriterResult for table {} (branch: {})",
          tp.partition(),
          tableContext.tableIdentifier(),
          tableContext.branch());
    } else {
      // Normal record: advance the committed offset one past this record.
      // Post-flag records in the same poll batch are already handled by the isPaused guard above —
      // once the flag is detected and pauseAssignment() is called, isPaused is true and subsequent
      // records in the same write() call are dropped here, to be re-delivered after resume.
      sourceOffsets.put(tp, new Offset(record.kafkaOffset() + 1, record.timestamp()));

      if (reroute != null) {
        // This path is reached only if reroute was set by a PREVIOUS (still-pending) flag from a
        // different partition.  Route this record to the flag's target table.
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
