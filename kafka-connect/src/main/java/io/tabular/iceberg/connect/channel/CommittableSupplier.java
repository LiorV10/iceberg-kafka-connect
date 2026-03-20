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
}
