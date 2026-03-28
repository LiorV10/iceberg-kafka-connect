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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import io.tabular.iceberg.connect.TableSinkConfig;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

public class IcebergWriterFactoryTest {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @SuppressWarnings("unchecked")
  public void testAutoCreateTable(boolean partitioned) {
    // Stub the created table so that the initial-snapshot commit (newAppend().commit())
    // doesn't NPE. This append is only called when the table is NEWLY created, not when
    // it is loaded by a subsequent worker — which is exactly the behaviour being tested.
    Table createdTable = mock(Table.class);
    AppendFiles appendFiles = mock(AppendFiles.class);
    when(createdTable.newAppend()).thenReturn(appendFiles);
    when(appendFiles.set(any(), any())).thenReturn(appendFiles);

    Catalog catalog = mock(Catalog.class);
    when(catalog.loadTable(any())).thenThrow(new NoSuchTableException("no such table"));
    when(catalog.createTable(any(), any(), any(), any())).thenReturn(createdTable);

    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    if (partitioned) {
      when(tableConfig.partitionBy()).thenReturn(ImmutableList.of("data"));
    }

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.autoCreateProps()).thenReturn(ImmutableMap.of("test-prop", "foo1"));
    when(config.tableConfig(any())).thenReturn(tableConfig);

    SinkRecord record = mock(SinkRecord.class);
    when(record.value()).thenReturn(ImmutableMap.of("id", 123, "data", "foo2"));

    IcebergWriterFactory factory = new IcebergWriterFactory(catalog, config);
    factory.autoCreateTable("db.tbl", record);

    ArgumentCaptor<TableIdentifier> identCaptor = ArgumentCaptor.forClass(TableIdentifier.class);
    ArgumentCaptor<Schema> schemaCaptor = ArgumentCaptor.forClass(Schema.class);
    ArgumentCaptor<PartitionSpec> specCaptor = ArgumentCaptor.forClass(PartitionSpec.class);
    ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);

    verify(catalog)
        .createTable(
            identCaptor.capture(),
            schemaCaptor.capture(),
            specCaptor.capture(),
            propsCaptor.capture());

    assertThat(identCaptor.getValue()).isEqualTo(TableIdentifier.of("db", "tbl"));
    assertThat(schemaCaptor.getValue().findField("id").type()).isEqualTo(LongType.get());
    assertThat(schemaCaptor.getValue().findField("data").type()).isEqualTo(StringType.get());
    assertThat(specCaptor.getValue().isPartitioned()).isEqualTo(partitioned);
    assertThat(propsCaptor.getValue()).containsKey("test-prop");

    // The initial empty snapshot must be committed exactly once — by the task that
    // created the table — and NOT by tasks that merely loaded an existing table.
    verify(createdTable).newAppend();
    verify(appendFiles).commit();
  }

  /**
   * When a table already exists (loaded by a subsequent worker after another task created it),
   * autoCreateTable must NOT commit an additional empty snapshot.  Without this guard,
   * every worker/partition that calls autoCreateTable would create one empty snapshot,
   * resulting in N snapshots on the initial commit.
   */
  @org.junit.jupiter.api.Test
  public void testAutoCreateTable_doesNotCreateExtraSnapshotWhenTableAlreadyExists() {
    Table existingTable = mock(Table.class);

    Catalog catalog = mock(Catalog.class);
    // loadTable succeeds — table was already created by another worker
    when(catalog.loadTable(any())).thenReturn(existingTable);

    TableSinkConfig tableConfig = mock(TableSinkConfig.class);
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.autoCreateEnabled()).thenReturn(true);
    when(config.tableConfig(any())).thenReturn(tableConfig);

    SinkRecord record = mock(SinkRecord.class);
    when(record.value()).thenReturn(ImmutableMap.of("id", 123, "data", "foo2"));

    IcebergWriterFactory factory = new IcebergWriterFactory(catalog, config);
    factory.autoCreateTable("db.tbl", record);

    // createTable must NOT have been called
    org.mockito.Mockito.verify(catalog, org.mockito.Mockito.never())
        .createTable(any(), any(), any(), any());
    // newAppend (i.e. empty snapshot commit) must NOT have been called on the loaded table
    org.mockito.Mockito.verify(existingTable, org.mockito.Mockito.never()).newAppend();
  }
}
