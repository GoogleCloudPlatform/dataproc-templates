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

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSToJDBC.class);
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
    validateInput();
    try (SparkSession spark = SparkSession.builder().appName("GCS to JDBC").getOrCreate()) {
      Dataset<Row> dataset =
          spark.read().format(config.getInputFormat()).load(config.getInputLocation());

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
            JDBCOptions.JDBC_BATCH_INSERT_SIZE(), config.getBatchInsertSize()) // default is 1000
        .option(JDBCOptions.JDBC_DRIVER_CLASS(), config.getJDBCDriver())
        .mode(config.getSaveMode())
        .save();
  }

  public void validateInput() {}
}
