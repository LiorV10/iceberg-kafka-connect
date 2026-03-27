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
package io.tabular.iceberg.connect.data;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

/**
 * A {@link WriterResult} that represents a flag record seen by a worker.  Instead of real Iceberg
 * data files it carries a single sentinel {@link DataFile} whose path starts with
 * {@link #FLAG_MARKER_PREFIX}.  The coordinator uses this prefix to distinguish flag events from
 * ordinary data, skips the file when appending to the table, and broadcasts a
 * {@code CommitToTable} event with {@link
 * io.tabular.iceberg.connect.channel.Coordinator#FLAG_PROCESSED_SENTINEL_ID} so that workers can
 * resume the paused partitions.
 */
public class FlagWriterResult extends WriterResult {

  /** Path prefix embedded in the sentinel DataFile to mark this as a flag event. */
  public static final String FLAG_MARKER_PREFIX = "__flag__:";

  public FlagWriterResult(TableIdentifier tableIdentifier) {
    super(
        tableIdentifier,
        ImmutableList.of(buildFlagMarkerFile(tableIdentifier)),
        ImmutableList.of(),
        Types.StructType.of());
  }

  private static DataFile buildFlagMarkerFile(TableIdentifier tableIdentifier) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(FLAG_MARKER_PREFIX + tableIdentifier)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(0L)
        .withRecordCount(0L)
        .build();
  }
}
