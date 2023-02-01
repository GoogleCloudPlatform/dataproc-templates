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

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.util.HashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToGCS implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(JDBCToGCS.class);
  private final JDBCToGCSConfig config;
  HashMap<String, String> jdbcProperties = new HashMap<>();

  public JDBCToGCS(JDBCToGCSConfig config) {
    this.config = config;
  }

  public static JDBCToGCS of(String... args) {
    JDBCToGCSConfig config = JDBCToGCSConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new JDBCToGCS(config);
  }

  @Override
  public void runTemplate() {

    SparkSession spark =
        SparkSession.builder()
            .appName("Spark Template JDBCToGCS ")
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

    DataFrameWriter<Row> writer =
        inputData.write().mode(config.getGcsWriteMode()).format(config.getGcsOutputFormat());

    if (StringUtils.isNotBlank(config.getGcsPartitionColumn())) {
      LOGGER.info("Partitioning data by :{} cols", config.getGcsPartitionColumn());
      writer = writer.partitionBy(config.getGcsPartitionColumn());
    }

    writer.save(config.getGcsOutputLocation());
  }

  public void validateInput() {
    jdbcProperties.put(JDBCOptions.JDBC_URL(), config.getJdbcURL());
    jdbcProperties.put(JDBCOptions.JDBC_DRIVER_CLASS(), config.getJdbcDriverClassName());
    jdbcProperties.put(JDBCOptions.JDBC_TABLE_NAME(), config.getSQL());

    if (StringUtils.isNotBlank(config.getConcatedPartitionProps())) {
      jdbcProperties.put(JDBCOptions.JDBC_PARTITION_COLUMN(), config.getJdbcSQLPartitionColumn());
      jdbcProperties.put(JDBCOptions.JDBC_UPPER_BOUND(), config.getJdbcSQLUpperBound());
      jdbcProperties.put(JDBCOptions.JDBC_LOWER_BOUND(), config.getJdbcSQLLowerBound());
      jdbcProperties.put(JDBCOptions.JDBC_NUM_PARTITIONS(), config.getJdbcSQLNumPartitions());
    }
  }
}
