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
import io.tabular.iceberg.connect.data.Utilities;

import java.util.Collection;

import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskImpl implements Task, AutoCloseable {

  private final Catalog catalog;
  private final Writer writer;
  private final Committer committer;

  public TaskImpl(SinkTaskContext context, IcebergSinkConfig config) {
    this.catalog = Utilities.loadCatalog(config);
    this.writer = new Worker(config, catalog, context);
    this.committer = new CommitterImpl(context, config, catalog);
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    // Skip writing source records while we are waiting for the Coordinator to acknowledge the
    // flag (FLAG_PROCESSED_SENTINEL).  Source-topic partitions are paused via context.pause()
    // during this window, so records should not normally arrive; this guard ensures correctness
    // even in edge cases where Kafka Connect delivers a batch before the pause fully takes effect.
    // The committer loop must still run so it can poll the control topic for the sentinel.
    if (!writer.isAwaitingFlagProcessing()) {
      writer.write(sinkRecords);
    }
    committer.commit(writer);
  }

  @Override
  public void close() throws Exception {
    Utilities.close(writer);
    Utilities.close(committer);
    Utilities.close(catalog);
  }
}
