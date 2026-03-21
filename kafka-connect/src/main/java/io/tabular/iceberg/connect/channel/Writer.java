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

import java.util.Collection;
import org.apache.kafka.connect.sink.SinkRecord;

interface Writer extends CommittableSupplier {
  void write(Collection<SinkRecord> sinkRecords);

  /**
   * Returns {@code true} when this writer has detected a flag record and has sent the flag result
   * to the Coordinator (i.e. {@link #committable()} has been called), but has not yet received the
   * per-table {@code FLAG_PROCESSED_SENTINEL} from the Coordinator.  While in this state the
   * source-topic partitions are paused via {@link org.apache.kafka.connect.sink.SinkTaskContext}
   * and no new source records should be written.
   *
   * <p>The default implementation always returns {@code false}, so anonymous / lambda suppliers
   * that do not implement flag processing are unaffected.
   */
  default boolean isAwaitingFlagProcessing() {
    return false;
  }
}
