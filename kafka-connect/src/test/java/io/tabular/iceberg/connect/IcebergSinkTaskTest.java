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
package io.tabular.iceberg.connect;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.tabular.iceberg.connect.channel.Task;
import java.util.Collection;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

public class IcebergSinkTaskTest {

  /**
   * A testable subclass that skips real catalog / Kafka connection setup.
   */
  private static class TestableIcebergSinkTask extends IcebergSinkTask {
    @Override
    Task createTask() {
      return mock(Task.class);
    }
  }

  /**
   * Verifies that {@link IcebergSinkTask#open} resumes the newly-assigned partitions via the
   * {@link SinkTaskContext} before creating the new {@link Task}.
   *
   * <p>The {@code SinkTaskContext} is provided by Kafka Connect and persists across
   * {@code open()}/{@code close()} cycles — it is NOT reset when the task is restarted.
   * If the previous lifecycle paused partitions (e.g. during flag processing) and the connector
   * is then resumed via the REST API ({@code PUT /connectors/{name}/resume}), Kafka Connect
   * calls {@code stop()} followed by {@code open()} on the task but the context retains the
   * stale pause state.  Without an explicit {@code context.resume()} in {@code open()}, the
   * new {@link io.tabular.iceberg.connect.channel.Worker} (which starts with no flag state)
   * would never receive records for those partitions, leaving the connector permanently stuck.
   */
  @Test
  public void testOpenResumesPartitionsOnRestart() {
    SinkTaskContext mockContext = mock(SinkTaskContext.class);
    TopicPartition tp0 = new TopicPartition("src-topic", 0);
    TopicPartition tp1 = new TopicPartition("src-topic", 1);

    IcebergSinkTask sinkTask = new TestableIcebergSinkTask();
    sinkTask.initialize(mockContext);

    sinkTask.open(ImmutableList.of(tp0, tp1));

    // resume() must be called for all assigned partitions so that any stale context-level pause
    // (left over from a previous flag-processing cycle) is cleared before records are delivered.
    verify(mockContext).resume(tp0, tp1);
  }

  /**
   * Verifies that when {@code open()} is called with an empty partition list (e.g. after a
   * rebalance that revokes all partitions), no {@code resume()} call is made.
   */
  @Test
  public void testOpenWithEmptyPartitionsDoesNotCallResume() {
    SinkTaskContext mockContext = mock(SinkTaskContext.class);

    IcebergSinkTask sinkTask = new TestableIcebergSinkTask();
    sinkTask.initialize(mockContext);

    sinkTask.open(ImmutableList.of());

    verify(mockContext, never()).resume();
  }
}
