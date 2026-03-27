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

public interface Task {
  void put(Collection<SinkRecord> sinkRecords);

  /**
   * Polls the control topic for pending coordinator messages (e.g.
   * {@link org.apache.iceberg.connect.events.StartCommit}) and responds to them.
   *
   * <p>This is called both from {@link #put} (the normal path when source records arrive) and
   * from {@code IcebergSinkTask.preCommit()} (the fallback path when all source partitions are
   * paused so Kafka Connect never calls {@code put()}).  The {@code preCommit()} path is
   * essential for the flag-commit flow: when a flag record is detected the Worker pauses its
   * source partition(s) via {@link org.apache.kafka.connect.sink.SinkTaskContext#pause} and
   * calls {@link org.apache.kafka.connect.sink.SinkTaskContext#requestCommit()} to ask Kafka
   * Connect to run a commit cycle.  Kafka Connect honours that request by calling
   * {@code preCommit()} — even while the partitions are paused — so delegating to this method
   * from {@code preCommit()} ensures the Committer can receive and respond to the next
   * {@link org.apache.iceberg.connect.events.StartCommit} without waiting for source records.
   */
  void commit();
}
