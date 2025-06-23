/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataproc.templates.pubsub.internal;

import java.util.*;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Custom PubSub Table representation for Spark Streaming */
public class PubSubTable implements SupportsRead {

  private final StructType schema;
  private final Map<String, String> properties;

  public PubSubTable(StructType schema, Map<String, String> properties) {
    this.schema = schema;
    this.properties = properties;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
    return new PubSubScanBuilder(schema, caseInsensitiveStringMap);
  }

  @Override
  public String name() {
    return "PubSub";
  }

  @Override
  public StructType schema() {
    return this.schema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return new HashSet<>(List.of(TableCapability.MICRO_BATCH_READ));
  }
}
