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

import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.common.TopicPartition;

interface CommittableSupplier {

  /**
   * Prefix used to encode a pending-flag state in the Kafka consumer-group offset metadata field.
   * When a worker detects a flag and commits offset {@code F+1}, it also stores
   * {@code "FLAG_PENDING:<rawTableId>"} as the metadata for that partition.  On restart,
   * {@link CommitterImpl} reads this metadata and calls {@link #restorePendingFlagState} so the
   * worker can re-enter the paused state and re-queue the {@link
   * io.tabular.iceberg.connect.data.FlagWriterResult} for re-submission — without needing to
   * re-read the flag record from the source topic.
   */
  String FLAG_METADATA_PREFIX = "FLAG_PENDING:";

  Committable committable();

  /**
   * Called by {@link CommitterImpl} when it receives the per-table sentinel
   * {@link org.apache.iceberg.connect.events.CommitToTable} event that the Coordinator broadcasts
   * after collecting flag votes from <em>all</em> source partitions for a specific table and
   * executing the flag action (e.g. branch switch).  Only workers whose {@code reroute} table
   * matches {@code tableIdentifier} need to act; others are unaffected.
   *
   * <p>The default implementation is a no-op so that anonymous/lambda suppliers used in tests and
   * the CommitterImpl constructor do not need to implement it.
   */
  default void onFlagProcessed(TableIdentifier tableIdentifier) {}

  /**
   * Returns per-partition metadata strings that should be encoded in the committed consumer-group
   * offsets for this cycle.  When a flag is pending (detected but not yet processed by the
   * coordinator), returns {@code {flagPartition: "FLAG_PENDING:<rawTableId>"}}.  Otherwise returns
   * an empty map.
   */
  default Map<TopicPartition, String> pendingFlagMetadata() {
    return Collections.emptyMap();
  }

  /**
   * Called by {@link CommitterImpl} on startup when it detects {@link #FLAG_METADATA_PREFIX}
   * metadata in the committed consumer-group offsets.  The implementation should immediately pause
   * the given partition and re-queue a {@link io.tabular.iceberg.connect.data.FlagWriterResult} so
   * that the coordinator receives the flag vote again on the next commit cycle.
   *
   * @param tp             the source topic-partition whose committed offset carries FLAG_PENDING
   * @param tableId        the raw table identifier (may include branch suffix)
   * @param committedOffset the offset that was committed ({@code F+1} — one past the flag record)
   */
  default void restorePendingFlagState(
      TopicPartition tp, TableIdentifier tableId, long committedOffset) {}
}
