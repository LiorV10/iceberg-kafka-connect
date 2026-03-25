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
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.TableContext;
import io.tabular.iceberg.connect.data.FlagWriterResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class both de-duplicates a batch of envelopes and adds logging to help disambiguate between
 * different ways that duplicates could manifest. Duplicates could occur in the following three
 * general ways:
 *
 * <ul>
 *   <li>same file appears in 2 equivalent envelopes e.g. if the Coordinator read the same message
 *       twice from Kafka
 *       <ul>
 *         <li>In this case, you should see a log message similar to "Deduplicated 2 data files with
 *             the same path=data.parquet for table=db.tbl during
 *             commit-id=cf602430-0f4d-41d8-a3e9-171848d89832 from the following
 *             events=[2x(SimpleEnvelope{...})]",
 *       </ul>
 *   <li>same file appears in 2 different envelopes e.g. if a Worker sent the same message twice to
 *       Kafka
 *       <ul>
 *         <li>In this case, you should see a log message similar to "Deduplicated 2 data files with
 *             the same path=data.parquet for table=db.tbl during
 *             commit-id=cf602430-0f4d-41d8-a3e9-171848d89832 from the following
 *             events=[1x(SimpleEnvelope{...}), 1x(SimpleEnvelope{...})]",
 *       </ul>
 *   <li>same file appears in a single envelope twice e.g. if a Worker included the same file twice
 *       in a single message in Kafka
 *       <ul>
 *         <li>In this case, you should see a log message similar to "Deduplicated 2 data files with
 *             the same path=data.parquet in the same event=SimpleEnvelope{...} for table=db.tbl
 *             during commit-id=cf602430-0f4d-41d8-a3e9-171848d89832"
 *       </ul>
 * </ul>
 */
class Deduplicated {
  private static final Logger LOG = LoggerFactory.getLogger(Deduplicated.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Deduplicated() {}

  /**
   * Returns the deduplicated data files from the batch of envelopes. Does not guarantee anything
   * about the ordering of the files that are returned.
   */
  public static List<DataFile> dataFiles(
      UUID currentCommitId, TableIdentifier tableIdentifier, List<Envelope> envelopes) {
    return deduplicatedFiles(
        currentCommitId,
        tableIdentifier,
        envelopes,
        "data",
        DataWritten::dataFiles,
        dataFile -> dataFile.path().toString());
  }

  /**
   * Returns the deduplicated delete files from the batch of envelopes. Does not guarantee anything
   * about the ordering of the files that are returned.
   */
  public static List<DeleteFile> deleteFiles(
      UUID currentCommitId, TableIdentifier tableIdentifier, List<Envelope> envelopes) {
    return deduplicatedFiles(
        currentCommitId,
        tableIdentifier,
        envelopes,
        "delete",
        DataWritten::deleteFiles,
        deleteFile -> deleteFile.path().toString());
  }

  /**
   * Returns all flag messages from the batch of envelopes, deduplicated by flag type.
   * Flag messages are identified by containing sentinel data files with paths starting with
   * {@link FlagWriterResult#FLAG_PREFIX}.
   * The record value is deserialized from the JSON embedded in the DataFile path.
   * The map key is the flag type (extracted from the record using {@code flagTypeField}),
   * and the map value is a pair of the {@link TableContext} and the record <em>value</em> only
   * (the {@code "value"} field of the serialized envelope — not the full envelope).
   *
   * <p>In a multi-task deployment the producer broadcasts the same flag to every Kafka
   * partition so that every task sees it, sets its own reroute, and reports a
   * {@link FlagWriterResult} to the Coordinator. The Coordinator therefore receives one
   * flag per partition for the same logical flag event. This method deduplicates those
   * copies by flag type, keeping the first occurrence, so the Coordinator processes each
   * logical flag exactly once.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Pair<TableContext, Map<String, Object>>> flagMessages(
      UUID currentCommitId, TableIdentifier tableIdentifier, List<Envelope> envelopes, String regex,
      String flagTypeField) {
    return envelopes.stream()
        .map(envelope -> (DataWritten) envelope.event().payload())
        .filter(dataWritten -> {
          List<DataFile> dataFiles = dataWritten.dataFiles();

          if (dataFiles == null || dataFiles.isEmpty()) {
            return false;
          }

          return dataFiles.stream()
                  .allMatch(f -> f.path().toString().startsWith(FlagWriterResult.FLAG_PREFIX));
        })
        .collect(toMap(
                dataWritten -> extractFlagType(dataWritten, flagTypeField),
                dataWritten -> {
                  String recordJson = dataWritten.dataFiles().stream().findFirst().get()
                      .path().toString().substring(FlagWriterResult.FLAG_PREFIX.length());
                  Map<String, Object> envelope = parseRecordJson(recordJson);
                  Object valueObj = envelope.get("value");
                  Map<String, Object> recordValue =
                      valueObj instanceof Map
                          ? (Map<String, Object>) valueObj
                          : Collections.emptyMap();
                  TableContext tableContext = TableContext.parse(
                      dataWritten.tableReference().identifier(), regex);
                  return Pair.of(tableContext, recordValue);
                },
                // Deduplicate: when the same flag type is reported by multiple tasks (one per
                // partition in a broadcast scenario) keep the first occurrence and discard the rest.
                (existing, duplicate) -> {
                  LOG.debug("Deduplicating flag: type already seen, discarding copy from additional partition");
                  return existing;
                }));
  }

  /**
   * Returns the set of source partition numbers per flag type that are present in this batch
   * of envelopes. Each DataWritten event carrying flag files contributes the source partition
   * number embedded in the flag JSON. Using a Set ensures a single partition cannot be counted
   * more than once within the same commit cycle (idempotency on retry/redelivery).
   *
   * <p>The Coordinator accumulates these sets across commit cycles so that a partition is only
   * counted once even if its flag result arrives in different cycles.
   */
  static Map<String, Set<Integer>> flagMessageSourcePartitions(
      List<Envelope> envelopes, String flagTypeField) {
    return envelopes.stream()
        .map(envelope -> (DataWritten) envelope.event().payload())
        .filter(
            dataWritten -> {
              List<DataFile> dataFiles = dataWritten.dataFiles();
              return dataFiles != null
                  && !dataFiles.isEmpty()
                  && dataFiles.stream()
                      .allMatch(
                          f -> f.path().toString().startsWith(FlagWriterResult.FLAG_PREFIX));
            })
        .collect(
            Collectors.groupingBy(
                dw -> extractFlagType(dw, flagTypeField),
                Collectors.mapping(
                    Deduplicated::extractSourcePartition, Collectors.toSet())));
  }

  @SuppressWarnings("unchecked")
  private static String extractFlagType(DataWritten dataWritten, String flagTypeField) {
    String recordJson = dataWritten.dataFiles().stream().findFirst().get()
        .path().toString().substring(FlagWriterResult.FLAG_PREFIX.length());
    Map<String, Object> envelope = parseRecordJson(recordJson);
    // The type field lives inside the "value" sub-map of the envelope.
    Object valueObj = envelope.get("value");
    if (valueObj instanceof Map) {
      Object type = ((Map<String, Object>) valueObj).get(flagTypeField);
      return type != null ? type.toString() : "";
    }
    return "";
  }

  private static int extractSourcePartition(DataWritten dataWritten) {
    String recordJson =
        dataWritten
            .dataFiles()
            .get(0)
            .path()
            .toString()
            .substring(FlagWriterResult.FLAG_PREFIX.length());
    Map<String, Object> record = parseRecordJson(recordJson);
    Object partition = record.get("partition");
    return partition instanceof Number ? ((Number) partition).intValue() : -1;
  }

  private static Map<String, Object> parseRecordJson(String recordJson) {
    try {
      return MAPPER.readValue(recordJson, new TypeReference<Map<String, Object>>() {});
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static <F> List<F> deduplicatedFiles(
      UUID currentCommitId,
      TableIdentifier tableIdentifier,
      List<Envelope> envelopes,
      String fileType,
      Function<DataWritten, List<F>> extractFilesFromPayload,
      Function<F, String> extractPathFromFile) {
    List<Pair<F, SimpleEnvelope>> filesAndEnvelopes =
        envelopes.stream()
            .flatMap(
                envelope -> {
                  DataWritten payload = (DataWritten) envelope.event().payload();
                  List<F> files = extractFilesFromPayload.apply(payload);
                  if (files == null) {
                    return Stream.empty();
                  } else {
                    SimpleEnvelope simpleEnvelope = new SimpleEnvelope(envelope);
                    return deduplicate(
                            files,
                            extractPathFromFile,
                            (path, duplicateFiles) ->
                                duplicateFilesInSameEventMessage(
                                    path,
                                    duplicateFiles,
                                    fileType,
                                    simpleEnvelope,
                                    tableIdentifier,
                                    currentCommitId))
                        .stream()
                        .map(file -> Pair.of(file, simpleEnvelope));
                  }
                })
            .collect(toList());

    List<Pair<F, SimpleEnvelope>> result =
        deduplicate(
            filesAndEnvelopes,
            fileAndEnvelope -> extractPathFromFile.apply(fileAndEnvelope.first()),
            (path, duplicateFilesAndEnvelopes) ->
                duplicateFilesAcrossMultipleEventsMessage(
                    path, duplicateFilesAndEnvelopes, fileType, tableIdentifier, currentCommitId));

    return result.stream().map(Pair::first).collect(toList());
  }

  private static <T> List<T> deduplicate(
      List<T> elements,
      Function<T, String> keyExtractor,
      BiFunction<String, List<T>, String> logMessageFn) {
    return elements.stream()
        .collect(Collectors.groupingBy(keyExtractor, Collectors.toList()))
        .entrySet()
        .stream()
        .flatMap(
            entry -> {
              String key = entry.getKey();
              List<T> values = entry.getValue();
              if (values.size() > 1) {
                LOG.warn(logMessageFn.apply(key, values));
              }
              return Stream.of(values.get(0));
            })
        .collect(toList());
  }

  private static <F> String duplicateFilesInSameEventMessage(
      String path,
      List<F> duplicateFiles,
      String fileType,
      SimpleEnvelope envelope,
      TableIdentifier tableIdentifier,
      UUID currentCommitId) {
    return String.format(
        "Deduplicated %d %s files with the same path=%s in the same event=%s for table=%s during commit-id=%s",
        duplicateFiles.size(), fileType, path, envelope, tableIdentifier, currentCommitId);
  }

  private static <F> String duplicateFilesAcrossMultipleEventsMessage(
      String path,
      List<Pair<F, SimpleEnvelope>> duplicateFilesAndEnvelopes,
      String fileType,
      TableIdentifier tableIdentifier,
      UUID currentCommitId) {
    return String.format(
        "Deduplicated %d %s files with the same path=%s for table=%s during commit-id=%s from the following events=%s",
        duplicateFilesAndEnvelopes.size(),
        fileType,
        path,
        tableIdentifier,
        currentCommitId,
        duplicateFilesAndEnvelopes.stream()
            .map(Pair::second)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
            .entrySet()
            .stream()
            .map(e -> String.format("%dx(%s)", e.getValue(), e.getKey()))
            .collect(toList()));
  }

  private static class SimpleEnvelope {

    private final int partition;
    private final long offset;
    private final UUID eventId;
    private final String eventGroupId;
    private final OffsetDateTime eventTimestamp;
    private final UUID payloadCommitId;

    SimpleEnvelope(Envelope envelope) {
      partition = envelope.partition();
      offset = envelope.offset();
      eventId = envelope.event().id();
      eventGroupId = envelope.event().groupId();
      eventTimestamp = envelope.event().timestamp();
      payloadCommitId = ((DataWritten) envelope.event().payload()).commitId();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SimpleEnvelope that = (SimpleEnvelope) o;
      return partition == that.partition
          && offset == that.offset
          && Objects.equals(eventId, that.eventId)
          && Objects.equals(eventGroupId, that.eventGroupId)
          && Objects.equals(eventTimestamp, that.eventTimestamp)
          && Objects.equals(payloadCommitId, that.payloadCommitId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          partition, offset, eventId, eventGroupId, eventTimestamp, payloadCommitId);
    }

    @Override
    public String toString() {
      return "SimpleEnvelope{"
          + "partition="
          + partition
          + ", offset="
          + offset
          + ", eventId="
          + eventId
          + ", eventGroupId='"
          + eventGroupId
          + '\''
          + ", eventTimestamp="
          + eventTimestamp.toInstant().toEpochMilli()
          + ", payloadCommitId="
          + payloadCommitId
          + '}';
    }
  }
}
