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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.HashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToBigQuery implements BaseTemplate {

  public static final Logger LOGGER =
      LoggerFactory.getLogger(com.google.cloud.dataproc.templates.jdbc.JDBCToBigQuery.class);

  private String bqLocation;
  private String jdbcURL;
  private String jdbcDriverClassName;
  private String jdbcFetchSize;
  private String temporaryGcsBucket;
  // Default as ErrorIfExists
  private String bqWriteMode = "ErrorIfExists";
  private String jdbcSQL;
  private String jdbcSQLPartitionColumn;
  private String jdbcSQLLowerBound;
  private String jdbcSQLUpperBound;
  private String jdbcSQLNumPartitions;
  private String concatedPartitionProps;
  private String tempTable;
  private String tempQuery;
  private final String sparkLogLevel;

  public JDBCToBigQuery() {

    bqLocation = getProperties().getProperty(JDBC_TO_BQ_BIGQUERY_LOCATION);
    bqWriteMode = getProperties().getProperty(JDBC_TO_BQ_WRITE_MODE);
    temporaryGcsBucket = getProperties().getProperty(JDBC_TO_BQ_TEMP_GCS_BUCKET);
    jdbcURL = getProperties().getProperty(JDBC_TO_BQ_JDBC_URL);
    jdbcDriverClassName = getProperties().getProperty(JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME);
    jdbcFetchSize = getProperties().getProperty(JDBC_TO_BQ_JDBC_FETCH_SIZE);
    jdbcSQL =
        getSQL(
            getProperties().getProperty(JDBC_TO_BQ_JDBC_URL),
            getProperties().getProperty(JDBC_TO_BQ_SQL));
    jdbcSQLPartitionColumn = getProperties().getProperty(JDBC_TO_BQ_SQL_PARTITION_COLUMN);
    jdbcSQLLowerBound = getProperties().getProperty(JDBC_TO_BQ_SQL_LOWER_BOUND);
    jdbcSQLUpperBound = getProperties().getProperty(JDBC_TO_BQ_SQL_UPPER_BOUND);
    jdbcSQLNumPartitions = getProperties().getProperty(JDBC_TO_BQ_SQL_NUM_PARTITIONS);
    concatedPartitionProps =
        jdbcSQLPartitionColumn + jdbcSQLLowerBound + jdbcSQLUpperBound + jdbcSQLNumPartitions;
    tempTable = getProperties().getProperty(JDBC_BQ_TEMP_TABLE);
    tempQuery = getProperties().getProperty(JDBC_BQ_TEMP_QUERY);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  public String getSQL(String jdbcURL, String sqlQuery) {
    if (jdbcURL.contains("oracle")) {
      return "(" + sqlQuery + ")";
    } else {
      return "(" + sqlQuery + ") as a";
    }
  }

  @Override
  public void runTemplate() {

    validateInput();

    SparkSession spark = null;

    spark =
        SparkSession.builder()
            .appName("Spark JDBCToBigQuery Job")
            .config("temporaryGcsBucket", temporaryGcsBucket)
            .enableHiveSupport()
            .getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    HashMap<String, String> jdbcProperties = new HashMap<>();
    jdbcProperties.put(JDBCOptions.JDBC_URL(), jdbcURL);
    jdbcProperties.put(JDBCOptions.JDBC_DRIVER_CLASS(), jdbcDriverClassName);
    jdbcProperties.put(JDBCOptions.JDBC_URL(), jdbcURL);
    jdbcProperties.put(JDBCOptions.JDBC_TABLE_NAME(), jdbcSQL);

    if (StringUtils.isNotBlank(concatedPartitionProps)) {
      jdbcProperties.put(JDBCOptions.JDBC_PARTITION_COLUMN(), jdbcSQLPartitionColumn);
      jdbcProperties.put(JDBCOptions.JDBC_UPPER_BOUND(), jdbcSQLUpperBound);
      jdbcProperties.put(JDBCOptions.JDBC_LOWER_BOUND(), jdbcSQLLowerBound);
      jdbcProperties.put(JDBCOptions.JDBC_NUM_PARTITIONS(), jdbcSQLNumPartitions);
    }

    /** Read Input data from JDBC table */
    Dataset<Row> inputData = spark.read().format("jdbc").options(jdbcProperties).load();

    if (StringUtils.isNotBlank(tempTable) && StringUtils.isNotBlank(tempQuery)) {
      inputData.createOrReplaceGlobalTempView(tempTable);
      inputData = spark.sql(tempQuery);
    }

    inputData
        .write()
        .mode(bqWriteMode)
        .format("com.google.cloud.spark.bigquery")
        .option("table", bqLocation)
        .save();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(bqLocation)
        || StringUtils.isAllBlank(jdbcURL)
        || StringUtils.isAllBlank(jdbcDriverClassName)
        || StringUtils.isAllBlank(jdbcSQL)
        || StringUtils.isAllBlank(temporaryGcsBucket)) {
      LOGGER.error(
          "{},{},{},{},{} are required parameters. ",
          JDBC_TO_BQ_BIGQUERY_LOCATION,
          JDBC_TO_BQ_JDBC_URL,
          JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME,
          JDBC_TO_BQ_SQL,
          JDBC_TO_BQ_TEMP_GCS_BUCKET);
      throw new IllegalArgumentException(
          "Required parameters for JDBCToBQ not passed. "
              + "Set mandatory parameter for JDBCToBQ template "
              + "in resources/conf/template.properties file or at runtime. Refer to jdbc/README.md for more instructions.");
    }

    if (StringUtils.isNotBlank(concatedPartitionProps)
        && ((StringUtils.isBlank(jdbcSQLPartitionColumn)
                || StringUtils.isBlank(jdbcSQLLowerBound)
                || StringUtils.isBlank(jdbcSQLUpperBound))
            || StringUtils.isBlank(jdbcSQLNumPartitions))) {
      throw new IllegalArgumentException(
          "Required parameters for JDBCToGCS not passed. "
              + "Set all the sql partitioning parameters together"
              + "in resources/conf/template.properties file or at runtime. Refer to jdbc/README.md for more instructions.");
    }
    LOGGER.info(
        "Starting JDBC to BigQuery spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {}:{}"
            + "5. {}:{}"
            + "6. {}:{}"
            + "7. {}:{}"
            + "8. {}:{}",
        JDBC_TO_BQ_BIGQUERY_LOCATION,
        bqLocation,
        JDBC_TO_BQ_WRITE_MODE,
        bqWriteMode,
        JDBC_TO_BQ_SQL,
        jdbcSQL,
        JDBC_TO_BQ_JDBC_FETCH_SIZE,
        jdbcFetchSize,
        JDBC_TO_BQ_SQL_PARTITION_COLUMN,
        jdbcSQLPartitionColumn,
        JDBC_TO_BQ_SQL_UPPER_BOUND,
        jdbcSQLUpperBound,
        JDBC_TO_BQ_SQL_LOWER_BOUND,
        jdbcSQLLowerBound,
        JDBC_TO_BQ_SQL_NUM_PARTITIONS,
        jdbcSQLNumPartitions);

    SparkSession spark = null;

    spark =
        SparkSession.builder()
            .appName("Spark JDBCToBigQuery Job")
            .config("temporaryGcsBucket", temporaryGcsBucket)
            .enableHiveSupport()
            .getOrCreate();

    HashMap<String, String> jdbcProperties = new HashMap<>();
    jdbcProperties.put(JDBCOptions.JDBC_URL(), jdbcURL);
    jdbcProperties.put(JDBCOptions.JDBC_DRIVER_CLASS(), jdbcDriverClassName);
    jdbcProperties.put(JDBCOptions.JDBC_URL(), jdbcURL);
    jdbcProperties.put(JDBCOptions.JDBC_TABLE_NAME(), jdbcSQL);

    if (StringUtils.isNotBlank(concatedPartitionProps)) {
      jdbcProperties.put(JDBCOptions.JDBC_PARTITION_COLUMN(), jdbcSQLPartitionColumn);
      jdbcProperties.put(JDBCOptions.JDBC_UPPER_BOUND(), jdbcSQLUpperBound);
      jdbcProperties.put(JDBCOptions.JDBC_LOWER_BOUND(), jdbcSQLLowerBound);
      jdbcProperties.put(JDBCOptions.JDBC_NUM_PARTITIONS(), jdbcSQLNumPartitions);
    }

    if (StringUtils.isNotBlank(jdbcFetchSize)) {
      jdbcProperties.put(JDBCOptions.JDBC_BATCH_FETCH_SIZE(), jdbcFetchSize);
    }

    /** Read Input data from JDBC table */
    Dataset<Row> inputData = spark.read().format("jdbc").options(jdbcProperties).load();

    if (StringUtils.isNotBlank(tempTable) && StringUtils.isNotBlank(tempQuery)) {
      inputData.createOrReplaceGlobalTempView(tempTable);
      inputData = spark.sql(tempQuery);
    }

    inputData
        .write()
        .mode(bqWriteMode)
        .format("com.google.cloud.spark.bigquery")
        .option("table", bqLocation)
        .save();
  }
}
