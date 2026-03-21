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

import java.util.List;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

/**
 * A special WriterResult that represents a flag message.
 * It has no data files but carries branch switching information in the partition struct.
 * The partition struct will have a special field "__flag_branch__" containing the target branch name.
 * The entire flag record is serialized as JSON and embedded in the dummy DataFile path using the
 * format: {@link #FLAG_PREFIX} + recordJson.
 */
public class FlagWriterResult extends WriterResult {
  
  private static final String FLAG_BRANCH_FIELD = "__flag_branch__";
  public static final String FLAG_PREFIX = "__flag__";
  private final String targetBranch;

  public FlagWriterResult(TableIdentifier tableIdentifier, String targetBranch, String recordJson) {
    super(
        tableIdentifier,
            ImmutableList.of(
                    DataFiles.builder((PartitionSpec.unpartitioned()))
                            .withPath(FLAG_PREFIX.concat(recordJson))
                            .withFormat(FileFormat.PARQUET)
                            .withFileSizeInBytes(0L)
                            .withRecordCount(0)
                            .build()
            ),
        ImmutableList.of(), 
        createFlagPartitionStruct(targetBranch)
    );
    this.targetBranch = targetBranch;
  }

  private static Types.StructType createFlagPartitionStruct(String targetBranch) {
    return Types.StructType.of(
        Types.NestedField.optional(1, FLAG_BRANCH_FIELD, Types.StringType.get())
    );
  }

  public String targetBranch() {
    return targetBranch;
  }

  public boolean isFlagMessage() {
    return true;
  }

  /**
   * Check if a WriterResult is a flag message by examining its partition struct.
   */
  public static boolean isFlagMessage(WriterResult writerResult) {
    if (writerResult instanceof FlagWriterResult) {
      return true;
    }
    Types.StructType partitionStruct = writerResult.partitionStruct();
    return partitionStruct != null && 
           partitionStruct.fields().stream()
               .anyMatch(field -> FLAG_BRANCH_FIELD.equals(field.name()));
  }

  /**
   * Extract the target branch from a flag message WriterResult.
   */
  public static String extractTargetBranch(WriterResult writerResult) {
    if (writerResult instanceof FlagWriterResult) {
      return ((FlagWriterResult) writerResult).targetBranch();
    }
    // This would need to extract from the serialized partition struct
    // For now, return null as we'll use instanceof check
    return null;
  }
}
