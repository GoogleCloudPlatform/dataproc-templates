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
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToGCS implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(JDBCToGCS.class);

  private String GCSOutputLocation;
  // Choosing default write mode as overwrite
  private String GCSWriteMode = "OVERWRITE";
  private String GCSPartitionColumn;
  // Default output format as csv
  private String GCSOutputFormat = "csv";
  private String jdbcURL;
  private String jdbcDriverClassName;
  private String jdbcSQL;

  public JDBCToGCS() {

    GCSOutputLocation = getProperties().getProperty(JDBC_TO_GCS_OUTPUT_LOCATION);
    jdbcSQL = getProperties().getProperty(JDBC_TO_GCS_SQL);
    jdbcURL = getProperties().getProperty(JDBC_TO_GCS_JDBC_URL);
    jdbcDriverClassName = getProperties().getProperty(JDBC_TO_GCS_JDBC_DRIVER_CLASS_NAME);
    GCSPartitionColumn = getProperties().getProperty(JDBC_TO_GCS_PARTITION_COLUMN);
    GCSOutputFormat = getProperties().getProperty(JDBC_TO_GCS_OUTPUT_FORMAT);
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(GCSOutputLocation)
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

    LOGGER.info(
        "Starting JDBC to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}",
        JDBC_TO_GCS_OUTPUT_LOCATION,
        GCSOutputLocation,
        JDBC_TO_GCS_WRITE_MODE,
        GCSWriteMode,
        JDBC_TO_GCS_JDBC_URL,
        jdbcURL);

    SparkSession spark =
        SparkSession.builder()
            .appName("Spark Template JDBCToGCS ")
            .enableHiveSupport()
            .getOrCreate();

    /** Read Input data from JDBC table */
    Dataset<Row> inputData =
        spark
            .read()
            .format("jdbc")
            .option("url", jdbcURL)
            .option("driver", jdbcDriverClassName)
            .option("query", jdbcSQL)
            .load();

    DataFrameWriter<Row> writer = inputData.write().mode(GCSWriteMode).format(GCSOutputFormat);

    /*
     * If optional partition column is passed than partition data by partition
     * column before writing to GCS.
     * */
    if (StringUtils.isNotBlank(GCSPartitionColumn)) {
      LOGGER.info("Partitioning data by :{} cols", GCSPartitionColumn);
      writer = writer.partitionBy(GCSPartitionColumn);
    }

    writer.save(GCSOutputLocation);
  }
}
