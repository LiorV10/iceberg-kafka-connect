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

interface CommittableSupplier {
  Committable committable();

  /**
   * Called by {@link CommitterImpl} when the {@link Coordinator} signals that all pending flag
   * actions for the current commit cycle have been executed (i.e. the branch switch is done).
   * Workers should resume their paused source-topic partitions and process any records that were
   * buffered after the flag record.
   *
   * <p>The default implementation is a no-op so that anonymous/lambda suppliers used in tests and
   * the CommitterImpl constructor do not need to implement it.
   */
  default void onFlagProcessed() {}
}
