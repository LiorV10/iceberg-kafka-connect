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

import org.apache.iceberg.catalog.TableIdentifier;

interface CommittableSupplier {
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
   * Returns {@code true} when all assigned source partitions have been paused due to flag
   * processing and the flag has not yet been fully processed (i.e. {@link #onFlagProcessed}
   * has not been called with the matching table).
   *
   * <p>When this returns {@code true}, the Kafka Connect framework will not call
   * {@code SinkTask.put()} because there are no records to deliver.  Since
   * {@link CommitterImpl#commit} is called from {@code put()}, the control topic would never
   * be polled again — creating a deadlock where the {@code onFlagProcessed} sentinel is never
   * received and the partitions are never unpaused.
   *
   * <p>{@link CommitterImpl} uses this method to detect the deadlock condition and keep polling
   * the control topic in a loop until the flag is processed and partitions are unpaused.
   *
   * <p>The default returns {@code false} so that lambda/anonymous suppliers in tests and the
   * CommitterImpl constructor do not need to implement it.
   */
  default boolean isAllPartitionsPaused() {
    return false;
  }

  /**
   * Drains any pending flag results that should be sent eagerly to the Coordinator without
   * waiting for a {@code START_COMMIT} event.
   *
   * <p><strong>Why this exists:</strong> {@link #committable()} is only called when the
   * CommitterImpl receives a {@code START_COMMIT} event from the Coordinator.  After a task
   * or pod restart, {@code START_COMMIT} may not arrive for up to {@code commitIntervalMs}
   * because the Coordinator's timer also restarts.  Without this method, the flag result
   * would be stranded in the Worker until the next {@code START_COMMIT} arrives — which in
   * the worst case could be several minutes.  Before a restart, this timing gap doesn't
   * exist because {@code START_COMMIT} events flow regularly in the steady state.
   *
   * <p>Unlike {@link #committable()}, this method only removes flag results from the worker —
   * normal write results and source offsets are left untouched so they can still be included
   * in the next regular commit cycle.  The returned {@link Committable} has empty offsets so
   * no source-topic offsets are committed for the flag partitions.
   *
   * <p>If the pending pause has not yet been applied, this method also applies it (via
   * {@code context.pause()}) to prevent further records from arriving on flagged partitions.
   *
   * @return a {@link Committable} containing only the pending flag writer results, or
   *         {@code null} if there are no pending flags.
   */
  default Committable drainPendingFlagCommittable() {
    return null;
  }
}
