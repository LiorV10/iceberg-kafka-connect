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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class Offset {

  public static final String PAUSED_METADATA = "paused";

  private final Long offset;
  private final Long timestamp;
  private final boolean paused;

  public Offset(Long offset, Long timestamp) {
    this(offset, timestamp, false);
  }

  public Offset(Long offset, Long timestamp, boolean paused) {
    Preconditions.checkNotNull(offset, "offset cannot be null");

    this.offset = offset;
    this.timestamp = timestamp;
    this.paused = paused;
  }

  public Long offset() {
    return offset;
  }

  public boolean paused() {
    return paused;
  }

  public OffsetDateTime timestamp() {
    if (timestamp == null) {
      return null;
    }
    return OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Offset offset1 = (Offset) o;
    return Objects.equals(offset, offset1.offset)
        && Objects.equals(timestamp, offset1.timestamp)
        && paused == offset1.paused;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, timestamp, paused);
  }

  @Override
  public String toString() {
    return "Offset{" + "offset=" + offset + ", timestamp=" + timestamp + ", paused=" + paused + '}';
  }
}
