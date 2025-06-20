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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.PUBSUB_DATASOURCE_SHORT_NAME;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Registering custom spark streaming read format Check:
 * src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
 */
public class PubSubSourceProvider implements TableProvider, DataSourceRegister {

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap caseInsensitiveStringMap) {
    return PubSubSchema.getSchema();
  }

  @Override
  public Table getTable(StructType schema, Transform[] transforms, Map<String, String> properties) {
    return new PubSubTable(schema, new CaseInsensitiveStringMap(properties));
  }

  @Override
  public String shortName() {
    return PUBSUB_DATASOURCE_SHORT_NAME;
  }
}
