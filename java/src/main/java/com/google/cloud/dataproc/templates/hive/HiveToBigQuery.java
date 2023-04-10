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
package com.google.cloud.dataproc.templates.hive;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark job to move data or/and schema from Hive table to BigQuery. This template can be configured
 * to run in few different modes. In default mode hivetobq.append.mode is set to ErrorIfExists. This
 * will cause failure if target BigQuery table already exists. Other possible values for this
 * property are: 1. Append 2. Overwrite 3. ErrorIfExists 4. Ignore For detailed list of properties
 * refer "HiveToBQ Template properties" section in resources/template.properties file.
 */
public class HiveToBigQuery implements BaseTemplate {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveToBigQuery.class);
  private String bqLocation;
  private String temporaryGcsBucket;
  private String hiveSQL;
  private String bqAppendMode;
  private String tempTable;
  private String tempQuery;
  private final String sparkLogLevel;

  public HiveToBigQuery() {
    bqLocation = getProperties().getProperty(HIVE_TO_BQ_BIGQUERY_LOCATION);
    temporaryGcsBucket = getProperties().getProperty(HIVE_TO_BQ_TEMP_GCS_BUCKET);
    hiveSQL = getProperties().getProperty(HIVE_TO_BQ_SQL);
    bqAppendMode = getProperties().getProperty(HIVE_TO_BQ_APPEND_MODE);
    tempTable = getProperties().getProperty(HIVE_TO_BQ_TEMP_TABLE);
    tempQuery = getProperties().getProperty(HIVE_TO_BQ_TEMP_QUERY);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {

    validateInput();

    // Initialize Spark session
    SparkSession spark =
        SparkSession.builder()
            .appName("Spark HiveToBigQuery Job")
            .config("temporaryGcsBucket", temporaryGcsBucket)
            .enableHiveSupport()
            .getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    LOGGER.debug("added jars : {}", spark.sparkContext().addedJars().keys());

    /** Read Input data from Hive table */
    Dataset<Row> inputData = spark.sql(hiveSQL);

    if (StringUtils.isNotBlank(tempTable) && StringUtils.isNotBlank(tempQuery)) {
      inputData.createOrReplaceGlobalTempView(tempTable);
      inputData = spark.sql(tempQuery);
    }

    inputData
        .write()
        .mode(bqAppendMode.toLowerCase())
        .format("com.google.cloud.spark.bigquery")
        .option("table", bqLocation)
        .save();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(bqLocation)
        || StringUtils.isAllBlank(hiveSQL)
        || StringUtils.isAllBlank(temporaryGcsBucket)) {
      LOGGER.error(
          "{},{},{} is required parameter. ",
          HIVE_TO_BQ_BIGQUERY_LOCATION,
          HIVE_TO_BQ_SQL,
          HIVE_TO_BQ_TEMP_GCS_BUCKET);
      throw new IllegalArgumentException(
          "Required parameters for HiveToBigQuery not passed. "
              + "Set mandatory parameter for HiveToBigQuery template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting Hive to BigQuery spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}",
        HIVE_TO_BQ_BIGQUERY_LOCATION,
        bqLocation,
        HIVE_TO_BQ_TEMP_GCS_BUCKET,
        temporaryGcsBucket,
        HIVE_TO_BQ_SQL,
        hiveSQL,
        HIVE_TO_BQ_APPEND_MODE,
        bqAppendMode);
  }
}
