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
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToGCS implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(JDBCToGCS.class);

  private String gcsOutputLocation;
  // Choosing default write mode as overwrite
  private String gcsWriteMode = "OVERWRITE";
  private String gcsPartitionColumn;
  // Default output format as csv
  private String gcsOutputFormat = "csv";
  private String jdbcURL;
  private String jdbcDriverClassName;
  private String jdbcSQL;
  private String jdbcSQLPartitionColumn;
  private String jdbcSQLLowerBound;
  private String jdbcSQLUpperBound;
  private String jdbcSQLNumPartitions;
  private String concatedPartitionProps;

  public JDBCToGCS() {

    gcsOutputLocation = getProperties().getProperty(JDBC_TO_GCS_OUTPUT_LOCATION);
    jdbcSQL = "(" + getProperties().getProperty(JDBC_TO_GCS_SQL) + ") as a";
    jdbcURL = getProperties().getProperty(JDBC_TO_GCS_JDBC_URL);
    jdbcDriverClassName = getProperties().getProperty(JDBC_TO_GCS_JDBC_DRIVER_CLASS_NAME);
    gcsPartitionColumn = getProperties().getProperty(JDBC_TO_GCS_OUTPUT_PARTITION_COLUMN);
    gcsOutputFormat = getProperties().getProperty(JDBC_TO_GCS_OUTPUT_FORMAT);
    gcsWriteMode = getProperties().getProperty(JDBC_TO_GCS_WRITE_MODE);

    jdbcSQLPartitionColumn = getProperties().getProperty(JDBC_TO_GCS_SQL_PARTITION_COLUMN);
    jdbcSQLLowerBound = getProperties().getProperty(JDBC_TO_GCS_SQL_LOWER_BOUND);
    jdbcSQLUpperBound = getProperties().getProperty(JDBC_TO_GCS_SQL_UPPER_BOUND);
    jdbcSQLNumPartitions = getProperties().getProperty(JDBC_TO_GCS_SQL_NUM_PARTITIONS);
    concatedPartitionProps =
        jdbcSQLPartitionColumn + jdbcSQLLowerBound + jdbcSQLUpperBound + jdbcSQLNumPartitions;
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(gcsOutputLocation)
        || StringUtils.isAllBlank(jdbcURL)
        || StringUtils.isAllBlank(jdbcDriverClassName)
        || StringUtils.isAllBlank(jdbcSQL)) {
      LOGGER.error(
          "{},{},{},{} are required parameters. ",
          JDBC_TO_GCS_OUTPUT_LOCATION,
          JDBC_TO_GCS_JDBC_URL,
          JDBC_TO_GCS_JDBC_DRIVER_CLASS_NAME,
          JDBC_TO_GCS_SQL);
      throw new IllegalArgumentException(
          "Required parameters for JDBCToGCS not passed. "
              + "Set mandatory parameter for JDBCToGCS template "
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
        "Starting JDBC to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {}:{}"
            + "5. {}:{}"
            + "6. {}:{}",
        JDBC_TO_GCS_OUTPUT_LOCATION,
        gcsOutputLocation,
        JDBC_TO_GCS_WRITE_MODE,
        gcsWriteMode,
        JDBC_TO_GCS_SQL_PARTITION_COLUMN,
        jdbcSQLPartitionColumn,
        JDBC_TO_GCS_SQL_UPPER_BOUND,
        jdbcSQLUpperBound,
        JDBC_TO_GCS_SQL_LOWER_BOUND,
        jdbcSQLLowerBound,
        JDBC_TO_GCS_SQL_NUM_PARTITIONS,
        jdbcSQLNumPartitions);

    SparkSession spark =
        SparkSession.builder()
            .appName("Spark Template JDBCToGCS ")
            .enableHiveSupport()
            .getOrCreate();

    /** Read Input data from JDBC table */
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

    Dataset<Row> inputData = spark.read().format("jdbc").options(jdbcProperties).load();

    DataFrameWriter<Row> writer = inputData.write().mode(gcsWriteMode).format(gcsOutputFormat);

    /*
     * If optional partition column is passed than partition data by partition
     * column before writing to GCS.
     * */
    if (StringUtils.isNotBlank(gcsPartitionColumn)) {
      LOGGER.info("Partitioning data by :{} cols", gcsPartitionColumn);
      writer = writer.partitionBy(gcsPartitionColumn);
    }

    writer.save(gcsOutputLocation);
  }
}
