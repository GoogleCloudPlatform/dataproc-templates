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

public class GCSToSpanner implements BaseTemplate {

  public static final String SPANNER_JDBC_DRIVER = "com.google.cloud.spanner.jdbc.JdbcDriver";

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSToSpanner.class);
  private final GCSToSpannerConfig config;

  public GCSToSpanner(GCSToSpannerConfig config) {
    this.config = config;
  }

  public static GCSToSpanner of(String... args) {
    GCSToSpannerConfig config = GCSToSpannerConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new GCSToSpanner(config);
  }

  @Override
  public void runTemplate() {
    validateInput();
    try (SparkSession spark = SparkSession.builder().appName("GCS to Spanner").getOrCreate()) {
      // Set log level
      spark.sparkContext().setLogLevel(config.getSparkLogLevel());

      Dataset<Row> dataset =
          spark.read().format(config.getInputFormat()).load(config.getInputLocation());
      write(dataset);
    }
  }

  public void write(Dataset<Row> dataset) {
    String spannerUrl =
        String.format(
            "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s?lenient=true",
            config.getProjectId(), config.getInstance(), config.getDatabase());
    JdbcDialects.registerDialect(new SpannerJdbcDialect());
    dataset
        .write()
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL(), spannerUrl)
        .option(JDBCOptions.JDBC_TABLE_NAME(), config.getTable())
        .option(
            JDBCOptions.JDBC_CREATE_TABLE_OPTIONS(),
            String.format("PRIMARY KEY (%s)", config.getPrimaryKey()))
        .option(
            JDBCOptions.JDBC_TXN_ISOLATION_LEVEL(),
            "NONE") // Needed because transaction have a 20,000 mutation limit per commit.
        .option(
            JDBCOptions.JDBC_BATCH_INSERT_SIZE(), config.getBatchInsertSize()) // default is 1000
        .option(JDBCOptions.JDBC_DRIVER_CLASS(), SPANNER_JDBC_DRIVER)
        .mode(config.getSaveMode())
        .save();
  }

  public void validateInput() {}
}
