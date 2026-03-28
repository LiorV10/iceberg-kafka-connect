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
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

interface Writer extends CommittableSupplier {
  void write(Collection<SinkRecord> sinkRecords);

  /**
   * Restores the paused/rerouted state from persistent offset metadata detected at startup.
   * Each entry maps a source {@link TopicPartition} to the table identifier that was being
   * rerouted when the offset was committed.  Implementations should set their reroute target
   * and pause the corresponding partitions.
   *
   * <p>The default implementation is a no-op.
   */
  default void restorePausedState(Map<TopicPartition, String> pendingFlags) {}
}
