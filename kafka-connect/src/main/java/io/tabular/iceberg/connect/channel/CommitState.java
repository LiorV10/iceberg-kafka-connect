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

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitState {
  private static final Logger LOG = LoggerFactory.getLogger(CommitState.class);

  private final List<Envelope> commitBuffer = new LinkedList<>();
  private final List<DataComplete> readyBuffer = new LinkedList<>();
  private long startTime;
  private UUID currentCommitId;
  private final IcebergSinkConfig config;

  private final Comparator<OffsetDateTime> dateTimeComparator =
          OffsetDateTime::compareTo;

  public CommitState(IcebergSinkConfig config) {
    this.config = config;
  }

  public void addResponse(Envelope envelope) {
    commitBuffer.add(envelope);
    if (!isCommitInProgress()) {
      LOG.debug(
          "Received data written with commit-id={} when no commit in progress, this can happen during recovery",
          ((DataWritten) envelope.event().payload()).commitId());
    }
  }

  public void addReady(Envelope envelope) {
    readyBuffer.add((DataComplete) envelope.event().payload());
    if (!isCommitInProgress()) {
      LOG.debug(
          "Received data complete for commit-id={} when no commit in progress, this can happen during recovery",
          ((DataComplete) envelope.event().payload()).commitId());
    }
  }

  public UUID currentCommitId() {
    return currentCommitId;
  }

  public boolean isCommitInProgress() {
    return currentCommitId != null;
  }

  public boolean isCommitIntervalReached() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    return (!isCommitInProgress()
        && System.currentTimeMillis() - startTime >= config.commitIntervalMs());
  }

  public void startNewCommit() {
    currentCommitId = UUID.randomUUID();
    startTime = System.currentTimeMillis();
  }

  public void endCurrentCommit() {
    readyBuffer.clear();
    currentCommitId = null;
  }

  public void clearResponses() {
    commitBuffer.clear();
  }

  public boolean isCommitTimedOut() {
    if (!isCommitInProgress()) {
      return false;
    }

    long currentTime = System.currentTimeMillis();
    if (currentTime - startTime > config.commitTimeoutMs()) {
      LOG.info("Commit timeout reached. Now: {}, start: {}, timeout: {}", currentTime, startTime, config.commitTimeoutMs());
      return true;
    }
    return false;
  }

  public boolean isCommitReady(int expectedPartitionCount) {
    if (!isCommitInProgress()) {
      return false;
    }

    int receivedPartitionCount =
        readyBuffer.stream()
            .filter(payload -> payload.commitId().equals(currentCommitId))
            .mapToInt(payload -> payload.assignments().size())
            .sum();

    if (receivedPartitionCount >= expectedPartitionCount) {
      LOG.info(
          "Commit {} ready, received responses for all {} partitions",
          currentCommitId,
          receivedPartitionCount);
      return true;
    }

    LOG.info(
        "Commit {} not ready, received responses for {} of {} partitions, waiting for more",
        currentCommitId,
        receivedPartitionCount,
        expectedPartitionCount);

    return false;
  }

  /**
   * Groups the buffered DataWritten envelopes first by target table and then by the commit-id
   * carried in each envelope's payload.
   *
   * <p>The buffer can contain envelopes from more than one commit-id: a commit that fails after
   * partial work resets the current commit (via {@link #endCurrentCommit()}) but does not clear the
   * buffer (only a fully successful commit calls {@link #clearResponses()}), so the next commit
   * cycle's envelopes accumulate on top of the previous cycle's. Grouping by commit-id lets the
   * coordinator commit each commit-id as its own Iceberg snapshot, in chronological order, so that
   * equality deletes from a later commit land in a later snapshot (with a higher sequence number)
   * than the data files they are meant to remove. If the two cycles were squashed into a single
   * RowDelta they would share one sequence number and the deletes would be ineffective.
   *
   * <p>Both the outer (table) and inner (commit-id) maps preserve first-seen insertion order via
   * {@link LinkedHashMap}, so iterating the inner map yields commit-ids in the order they were
   * received -- i.e. chronological commit order. The commit-id UUID value itself is random and must
   * not be used for ordering; only insertion order conveys the chronological signal.
   */
  public Map<TableIdentifier, Map<UUID, List<Envelope>>> tableCommitMap() {
    Map<TableIdentifier, Map<UUID, List<Envelope>>> result = new LinkedHashMap<>();
    for (Envelope envelope : commitBuffer) {
      DataWritten payload = (DataWritten) envelope.event().payload();
      TableIdentifier table = payload.tableReference().identifier();
      UUID commitId = payload.commitId();
      result
          .computeIfAbsent(table, k -> new LinkedHashMap<>())
          .computeIfAbsent(commitId, k -> new LinkedList<>())
          .add(envelope);
    }
    return result;
  }

  public OffsetDateTime vtts(boolean partialCommit) {
    return vttsForReady(readyBuffer, partialCommit);
  }

  /**
   * Computes the vtts (valid-through timestamp) watermark accumulatively up to and including the
   * given commit-ids, i.e. over the subset of buffered {@link DataComplete} payloads whose commit-id
   * is contained in {@code commitIds}.
   *
   * <p>This is used so that each per-commit-id snapshot records a vtts that reflects only the data
   * committed so far (this commit-id and all earlier ones in the batch), rather than the batch-wide
   * vtts. As with {@link #vtts(boolean)}, if any partition in the considered subset reports a null
   * timestamp the vtts is null.
   */
  public OffsetDateTime vttsUpTo(Collection<UUID> commitIds, boolean partialCommit) {
    Set<UUID> ids = new HashSet<>(commitIds);
    List<DataComplete> subset =
        readyBuffer.stream()
            .filter(payload -> ids.contains(payload.commitId()))
            .collect(Collectors.toList());
    return vttsForReady(subset, partialCommit);
  }

  private OffsetDateTime vttsForReady(List<DataComplete> ready, boolean partialCommit) {
    boolean validVtts =
        !partialCommit
            && ready.stream()
                .flatMap(event -> event.assignments().stream())
                .allMatch(offset -> offset.timestamp() != null);

      OffsetDateTime result;
      if (validVtts) {
          Optional<OffsetDateTime> maybeResult =
                  ready.stream()
                          .flatMap(event -> event.assignments().stream())
                          .map(TopicPartitionOffset::timestamp)
                          .min(dateTimeComparator);
          if (maybeResult.isPresent()) {
              result = maybeResult.get();
          } else {
              throw new NoSuchElementException("no vtts found");
          }
    } else {
      result = null;
    }
    return result;
  }
}
