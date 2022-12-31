/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.dataproc.templates.snowflake;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.util.HashMap;
import net.snowflake.spark.snowflake.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeToGCS implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeToGCS.class);
  private final SnowflakeToGCSConfig config;
  HashMap<String, String> properties = new HashMap<>();

  public SnowflakeToGCS(SnowflakeToGCSConfig config) {
    this.config = config;
  }

  public static SnowflakeToGCS of(String... args) {
    SnowflakeToGCSConfig config = SnowflakeToGCSConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new SnowflakeToGCS(config);
  }

  @Override
  public void runTemplate() {
    SparkSession spark = SparkSession.builder().appName("Snowflake To GCS").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(config.getSparkLogLevel());

    validateInput();

    Dataset<Row> inputData =
        spark.read().format(Utils.SNOWFLAKE_SOURCE_NAME()).options(properties).load();

    DataFrameWriter<Row> writer =
        inputData.write().mode(config.getGcsWriteMode()).format(config.getGcsWriteFormat());

    if (StringUtils.isNotBlank(config.getGcsPartitionColumn())) {
      writer = writer.partitionBy(config.getGcsPartitionColumn());
    }

    writer.save(config.getGcsLocation());
  }

  public void validateInput() {
    properties.put("sfURL", config.getSfUrl());
    properties.put("sfUser", config.getSfUser());
    properties.put("sfPassword", config.getSfPassword());
    properties.put("sfDatabase", config.getSfDatabase());
    properties.put("sfSchema", config.getSfSchema());
    properties.put("autopushdown", config.getSfAutoPushdown());

    if (StringUtils.isNotBlank(config.getSfWarehouse())) {
      properties.put("sfWarehouse", config.getSfWarehouse());
    }

    if (StringUtils.isNotBlank(config.getSfTable())) {
      properties.put("dbtable", config.getSfTable());
    } else {
      properties.put("query", config.getSfQuery());
    }
  }
}
