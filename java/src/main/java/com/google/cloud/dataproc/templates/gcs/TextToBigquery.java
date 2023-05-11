/*
 * Copyright (C) 2023 Google LLC
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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextToBigquery implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(TextToBigquery.class);

  private String projectID;
  private String inputLocation;
  private String inputCompression;
  private String inputDelimiter;
  private String outputDataset;
  private String outputTable;
  private String outputMode;
  private String bqTempBucket;
  private String bqTempTable;
  private String bqTempQuery;
  private final String sparkLogLevel;

  public TextToBigquery() {

    projectID = getProperties().getProperty(PROJECT_ID_PROP);
    inputLocation = getProperties().getProperty(TEXT_BIGQUERY_INPUT_LOCATION);
    inputCompression = getProperties().getProperty(TEXT_BIGQUERY_INPUT_COMPRESSION);
    inputDelimiter = getProperties().getProperty(TEXT_BIGQUERY_INPUT_DELIMITER);
    outputDataset = getProperties().getProperty(TEXT_BIGQUERY_OUTPUT_DATASET);
    outputTable = getProperties().getProperty(TEXT_BIGQUERY_OUTPUT_TABLE);
    outputMode = getProperties().getProperty(TEXT_BIGQUERY_OUTPUT_MODE);
    bqTempBucket = getProperties().getProperty(TEXT_BIGQUERY_TEMP_BUCKET);
    bqTempTable = getProperties().getProperty(TEXT_BIGQUERY_TEMP_TABLE);
    bqTempQuery = getProperties().getProperty(TEXT_BIGQUERY_TEMP_QUERY);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {

    SparkSession spark = null;

    spark = SparkSession.builder().appName("Text To Bigquery load").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    Dataset<Row> inputData = null;

    inputData =
        spark
            .read()
            .option(TEXT_BIGQUERY_HEADER_OPTION, true)
            .option(TEXT_BIGQUERY_INFERSCHEMA_OPTION, true)
            .option(TEXT_BIGQUERY_COMPRESSION_OPTION, inputCompression)
            .option(TEXT_BIGQUERY_DELIMITER_OPTION, inputDelimiter)
            .csv(inputLocation);

    if (bqTempTable != null && bqTempQuery != null) {
      inputData.createOrReplaceGlobalTempView(bqTempTable);
      inputData = spark.sql(bqTempQuery);
    }

    inputData
        .write()
        .format(SPARK_READ_FORMAT_BIGQUERY)
        .option(GCS_BQ_OUTPUT, outputDataset + "." + outputTable)
        .option(GCS_BQ_TEMP_BUCKET, bqTempBucket)
        .mode(SaveMode.valueOf(outputMode))
        .save();

    spark.stop();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(projectID)
        || StringUtils.isAllBlank(inputLocation)
        || StringUtils.isAllBlank(inputCompression)
        || StringUtils.isAllBlank(inputDelimiter)
        || StringUtils.isAllBlank(outputDataset)
        || StringUtils.isAllBlank(outputTable)
        || StringUtils.isAllBlank(outputMode)
        || StringUtils.isAllBlank(bqTempBucket)) {
      LOGGER.error(
          "{},{},{},{},{},{},{},{} are required parameter. ",
          PROJECT_ID_PROP,
          TEXT_BIGQUERY_INPUT_LOCATION,
          TEXT_BIGQUERY_INPUT_COMPRESSION,
          TEXT_BIGQUERY_INPUT_DELIMITER,
          TEXT_BIGQUERY_OUTPUT_DATASET,
          TEXT_BIGQUERY_OUTPUT_TABLE,
          TEXT_BIGQUERY_OUTPUT_MODE,
          TEXT_BIGQUERY_TEMP_BUCKET);
      throw new IllegalArgumentException(
          "Required parameters for TextToBigquery not passed. "
              + "Set mandatory parameter for TextToBigquery template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting Text To Bigquery spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {}:{}"
            + "5. {}:{}"
            + "6. {}:{}"
            + "7. {}:{}",
        TEXT_BIGQUERY_INPUT_LOCATION,
        inputLocation,
        TEXT_BIGQUERY_INPUT_DELIMITER,
        inputDelimiter,
        TEXT_BIGQUERY_INPUT_COMPRESSION,
        inputCompression,
        TEXT_BIGQUERY_OUTPUT_MODE,
        outputMode,
        TEXT_BIGQUERY_OUTPUT_DATASET,
        outputDataset,
        TEXT_BIGQUERY_OUTPUT_TABLE,
        outputTable,
        TEXT_BIGQUERY_TEMP_BUCKET,
        bqTempBucket);
  }
}
