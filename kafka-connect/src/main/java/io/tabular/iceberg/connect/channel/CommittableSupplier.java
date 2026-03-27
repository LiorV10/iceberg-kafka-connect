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
   * Called by {@link CommitterImpl} when it receives the per-table
   * {@link org.apache.iceberg.connect.events.CommitToTable} sentinel that the Coordinator
   * broadcasts after processing all flag events for {@code tableIdentifier}.  Implementations
   * should resume any source partitions that were paused for this table.
   *
   * <p>The default is a no-op so that anonymous/lambda suppliers used in tests and the
   * {@link CommitterImpl} constructor do not need to override this method.
   */
  default void onFlagProcessed(TableIdentifier tableIdentifier) {}
}
