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

import org.apache.spark.sql.types.*;

public class PubSubSchema {

  /**
   * We are not processing PubSub attributes for now. Generally, we don't store it at destination
   * sink.
   *
   * @return Spark dataframe schema
   */
  public static StructType getSchema() {
    return new StructType(
        new StructField[] {
          new StructField("ackId", DataTypes.StringType, false, Metadata.empty()),
          new StructField("data", DataTypes.StringType, true, Metadata.empty()),
          new StructField("messageId", DataTypes.StringType, false, Metadata.empty()),
          new StructField("publishTime", DataTypes.TimestampType, false, Metadata.empty())
        });
  }
}
