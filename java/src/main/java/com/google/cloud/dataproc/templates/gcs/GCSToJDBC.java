/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.dataproc.templates.gcs;

import com.google.cloud.dataproc.dialects.SpannerJdbcDialect;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSToJDBC implements BaseTemplate {
  public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSToSpanner.class);
  private final GCSToJDBCConfig config;

  public GCSToJDBC(GCSToJDBCConfig config) {
    this.config = config;
  }

  public static GCSToJDBC of(String... args) {
    GCSToJDBCConfig config = GCSToJDBCConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new GCSToJDBC(config);
  }

  @Override
  public void runTemplate() {
    try (SparkSession spark = SparkSession.builder().appName("GCS to JDBC").getOrCreate()) {
      Dataset<Row> dataset =
          spark.read().format(config.getInputFormat()).load(config.getInputLocation());
      LOGGER.info("Schema of input Data set is as follows ");
      dataset.printSchema();
      LOGGER.info("Actual Data is as follows ");
      dataset.show();

      write(dataset);
    }
  }

  public void write(Dataset<Row> dataset) {
    JdbcDialects.registerDialect(new SpannerJdbcDialect());
    dataset
        .write()
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL(), config.getJDBCUrl())
        .option(JDBCOptions.JDBC_TABLE_NAME(), config.getTable())
        .option(
            JDBCOptions.JDBC_TXN_ISOLATION_LEVEL(),
            "NONE") // Needed because transaction have a 20,000 mutation limit per commit.
        .option(
            JDBCOptions.JDBC_BATCH_INSERT_SIZE(), config.getBatchInsertSize()) // default is 1000
        .option(JDBCOptions.JDBC_DRIVER_CLASS(), JDBC_DRIVER)
        .mode(config.getSaveMode())
        .save();
  }
}
