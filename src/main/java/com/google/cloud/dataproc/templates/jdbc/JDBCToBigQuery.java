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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToBigQuery implements BaseTemplate {

  public static final Logger LOGGER =
      LoggerFactory.getLogger(com.google.cloud.dataproc.templates.jdbc.JDBCToBigQuery.class);

  private String bqLocation;
  private String jdbcURL;
  private String jdbcDriverClassName;
  private String temporaryGcsBucket;
  // Default as ErrorIfExists
  private String bqWriteMode = "ErrorIfExists";
  private String jdbcSQL;

  public JDBCToBigQuery() {

    bqLocation = getProperties().getProperty(JDBC_TO_BQ_BIGQUERY_LOCATION);
    bqWriteMode = getProperties().getProperty(JDBC_TO_BQ_WRITE_MODE);
    temporaryGcsBucket = getProperties().getProperty(JDBC_TO_BQ_TEMP_GCS_BUCKET);
    jdbcURL = getProperties().getProperty(JDBC_TO_BQ_JDBC_URL);
    jdbcDriverClassName = getProperties().getProperty(JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME);
    jdbcSQL = getProperties().getProperty(JDBC_TO_BQ_SQL);
  }

  @Override
  public void runTemplate() {
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

    LOGGER.info(
        "Starting JDBC to BQ spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}",
        JDBC_TO_BQ_BIGQUERY_LOCATION,
        bqLocation,
        JDBC_TO_BQ_WRITE_MODE,
        bqWriteMode,
        JDBC_TO_BQ_JDBC_URL,
        jdbcURL);

    SparkSession spark = null;
    LOGGER.info(
        "Starting JDBC to BigQuery spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}",
        JDBC_TO_BQ_BIGQUERY_LOCATION,
        bqLocation,
        JDBC_TO_BQ_SQL,
        jdbcSQL,
        JDBC_TO_BQ_WRITE_MODE,
        bqWriteMode,
        JDBC_TO_BQ_JDBC_URL,
        jdbcURL);

    spark =
        SparkSession.builder()
            .appName("Spark JDBCToBigQuery Job")
            .config("temporaryGcsBucket", temporaryGcsBucket)
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

    inputData.write().mode(bqWriteMode).format("bigquery").option("table", bqLocation).save();
  }
}
