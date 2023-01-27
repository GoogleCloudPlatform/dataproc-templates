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
package com.google.cloud.dataproc.templates.jdbc;

import com.google.cloud.dataproc.dialects.SpannerJdbcDialect;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.util.HashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToSpanner implements BaseTemplate {

  public static final String SPANNER_JDBC_DRIVER = "com.google.cloud.spanner.jdbc.JdbcDriver";

  public static final Logger LOGGER = LoggerFactory.getLogger(JDBCToSpanner.class);
  private final JDBCToSpannerConfig config;
  HashMap<String, String> jdbcProperties = new HashMap<>();

  public JDBCToSpanner(JDBCToSpannerConfig config) {
    this.config = config;
  }

  public static JDBCToSpanner of(String... args) {
    JDBCToSpannerConfig config = JDBCToSpannerConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new JDBCToSpanner(config);
  }

  @Override
  public void runTemplate() {

    SparkSession spark =
        SparkSession.builder()
            .appName("Spark Template JDBCToSpanner ")
            .enableHiveSupport()
            .getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(config.getSparkLogLevel());

    /** Read Input data from JDBC table */
    validateInput();

    if (StringUtils.isNotBlank(config.getJdbcFetchSize())) {
      jdbcProperties.put(JDBCOptions.JDBC_BATCH_FETCH_SIZE(), config.getJdbcFetchSize());
    }

    Dataset<Row> inputData = spark.read().format("jdbc").options(jdbcProperties).load();

    if (StringUtils.isNotBlank(config.getTempTable())
        && StringUtils.isNotBlank(config.getTempQuery())) {
      inputData.createOrReplaceGlobalTempView(config.getTempTable());
      inputData = spark.sql(config.getTempQuery());
    }

    write(inputData);
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

  public void validateInput() {
    jdbcProperties.put(JDBCOptions.JDBC_URL(), config.getJdbcURL());
    jdbcProperties.put(JDBCOptions.JDBC_DRIVER_CLASS(), config.getJdbcDriverClassName());
    jdbcProperties.put(JDBCOptions.JDBC_TABLE_NAME(), config.getSQL());

    if (StringUtils.isNotBlank(config.getJdbcSQLPartitionColumn())
        && StringUtils.isNotBlank(config.getJdbcSQLLowerBound())
        && StringUtils.isNotBlank(config.getJdbcSQLUpperBound())
        && StringUtils.isNotBlank(config.getJdbcSQLNumPartitions())) {
      jdbcProperties.put(JDBCOptions.JDBC_PARTITION_COLUMN(), config.getJdbcSQLPartitionColumn());
      jdbcProperties.put(JDBCOptions.JDBC_UPPER_BOUND(), config.getJdbcSQLUpperBound());
      jdbcProperties.put(JDBCOptions.JDBC_LOWER_BOUND(), config.getJdbcSQLLowerBound());
      jdbcProperties.put(JDBCOptions.JDBC_NUM_PARTITIONS(), config.getJdbcSQLNumPartitions());
    }
  }
}
