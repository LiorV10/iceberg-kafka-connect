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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class TableContext {
  private final TableIdentifier tableIdentifier;
  private final String branch;

  public static String parseBranch(String tableName, String regex) {
      return TableContext.parse(TableIdentifier.parse(tableName), regex).branch();
  }

  public static String parseName(String tableName, String regex) {
    return TableContext.parse(TableIdentifier.parse(tableName), regex).tableIdentifier().toString();
  }

  public static TableContext parse(TableIdentifier tableIdentifier, String regex) {
    Namespace namespace = tableIdentifier.namespace();
    String name = tableIdentifier.name();
    String branch = null;

    if (regex != null) {
      String[] nameParts = name.split(regex);
      if (nameParts.length == 2) {
        name = nameParts[0];
        branch = nameParts[1];
      }
    }

    return new TableContext(TableIdentifier.of(namespace, name), branch);
  }

  public TableContext(TableIdentifier tableIdentifier, String branch) {
    this.tableIdentifier = tableIdentifier;
    this.branch = branch;
  }

  public String branch() {
    return branch;
  }

  public TableIdentifier tableIdentifier() {
    return tableIdentifier;
  }
}
