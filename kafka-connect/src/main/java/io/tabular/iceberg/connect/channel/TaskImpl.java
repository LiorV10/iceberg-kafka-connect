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

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskImpl implements Task, AutoCloseable {

  private final Catalog catalog;
  private final Writer writer;
  private final Committer committer;
  // private final IcebergSinkConfig config;

  private static final Logger LOG = LoggerFactory.getLogger(TaskImpl.class);

  public TaskImpl(SinkTaskContext context, IcebergSinkConfig config) {
    this.catalog = Utilities.loadCatalog(config);
    this.writer = new Worker(config, catalog);
    this.committer = new CommitterImpl(context, config, catalog);
    // this.config = config;
  }

  private boolean handleFlagRecord(SinkRecord record) {
    if (!Utilities.isFlagRecord(record)) {
      return false;
    } else {
      LOG.debug("Handling flag record");
      // TODO: replace '__target_table' with config field value
      // TODO: replace 'dev' with config field value

      String targetId = Utilities.extractFromRecordValue(record, "__target_table").toString();
      Table targetTable = catalog.loadTable(TableIdentifier.parse(targetId));

      // targetTable.manageSnapshots().replaceBranch("").commit();
      targetTable.manageSnapshots().setCurrentSnapshot(targetTable.snapshot("dev").snapshotId()).commit();
    }

    return true;
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    // filter out flag records
    sinkRecords.removeIf(this::handleFlagRecord);

    writer.write(sinkRecords);
    committer.commit(writer);
  }

  @Override
  public void close() throws Exception {
    Utilities.close(writer);
    Utilities.close(committer);
    Utilities.close(catalog);
  }
}
