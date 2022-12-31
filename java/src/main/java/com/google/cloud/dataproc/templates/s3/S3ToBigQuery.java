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
package com.google.cloud.dataproc.templates.s3;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ToBigQuery implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(S3ToBigQuery.class);

  private String projectID;
  private String inputFileLocation;
  private String accessKey;
  private String accessSecret;
  private String bigQueryDataset;
  private String bigQueryTable;
  private String bqTempBucket;
  private String inputFileFormat;
  private final String sparkLogLevel;

  public S3ToBigQuery() {

    projectID = getProperties().getProperty(PROJECT_ID_PROP);
    inputFileLocation = getProperties().getProperty(S3_BQ_INPUT_LOCATION);
    accessKey = getProperties().getProperty(S3_BQ_ACCESS_KEY);
    accessSecret = getProperties().getProperty(S3_BQ_SECRET_KEY);
    bigQueryDataset = getProperties().getProperty(S3_BQ_OUTPUT_DATASET_NAME);
    bigQueryTable = getProperties().getProperty(S3_BQ_OUTPUT_TABLE_NAME);
    bqTempBucket = getProperties().getProperty(S3_BQ_LD_TEMP_BUCKET_NAME);
    inputFileFormat = getProperties().getProperty(S3_BQ_INPUT_FORMAT);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {

    validateInput();

    SparkSession spark = null;

    try {
      spark = SparkSession.builder().appName("S3 to Bigquery load").getOrCreate();

      // Set log level
      spark.sparkContext().setLogLevel(sparkLogLevel);

      spark.sparkContext().hadoopConfiguration().set(S3_BQ_ACCESS_KEY_CONFIG_NAME, accessKey);
      spark.sparkContext().hadoopConfiguration().set(S3_BQ_SECRET_KEY_CONFIG_NAME, accessSecret);
      spark
          .sparkContext()
          .hadoopConfiguration()
          .set(S3_BQ_ENDPOINT_CONFIG_NAME, S3_BQ_ENDPOINT_CONFIG_VALUE);

      Dataset<Row> inputData = null;

      switch (inputFileFormat) {
        case GCS_BQ_CSV_FORMAT:
          inputData =
              spark
                  .read()
                  .format(GCS_BQ_CSV_FORMAT)
                  .option(S3_BQ_HEADER, true)
                  .option(S3_BQ_INFER_SCHEMA, true)
                  .load(inputFileLocation);
          break;
        case S3_BQ_AVRO_FORMAT:
          inputData = spark.read().format(GCS_BQ_AVRO_EXTD_FORMAT).load(inputFileLocation);
          break;
        case S3_BQ_PRQT_FORMAT:
          inputData = spark.read().parquet(inputFileLocation);
          break;
        case S3_BQ_JSON_FORMAT:
          inputData =
              spark
                  .read()
                  .format(S3_BQ_JSON_FORMAT)
                  .option(S3_BQ_INFER_SCHEMA, true)
                  .load(inputFileLocation);
          break;
        default:
          throw new IllegalArgumentException(
              "Currenlty avro, parquet, json and csv are the only supported formats");
      }

      inputData
          .write()
          .format(S3_BQ_OUTPUT_FORMAT)
          .option(S3_BQ_HEADER, true)
          .option(S3_BQ_OUTPUT, bigQueryDataset + "." + bigQueryTable)
          .option(S3_BQ_TEMP_BUCKET, bqTempBucket)
          .mode(SaveMode.Append)
          .save();

    } catch (Throwable th) {
      LOGGER.error("Exception in S3toBigquery", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(projectID)
        || StringUtils.isAllBlank(inputFileLocation)
        || StringUtils.isAllBlank(accessKey)
        || StringUtils.isAllBlank(accessSecret)
        || StringUtils.isAllBlank(bigQueryDataset)
        || StringUtils.isAllBlank(bigQueryTable)
        || StringUtils.isAllBlank(bqTempBucket)
        || StringUtils.isAllBlank(inputFileFormat)) {
      LOGGER.error(
          "{},{},{},{},{},{},{},{} are required parameter. ",
          PROJECT_ID_PROP,
          S3_BQ_INPUT_LOCATION,
          S3_BQ_ACCESS_KEY,
          S3_BQ_SECRET_KEY_CONFIG_NAME,
          S3_BQ_OUTPUT_DATASET_NAME,
          S3_BQ_OUTPUT_TABLE_NAME,
          S3_BQ_LD_TEMP_BUCKET_NAME,
          S3_BQ_INPUT_FORMAT);
      throw new IllegalArgumentException(
          "Required parameters for S3toBQ not passed. "
              + "Set mandatory parameter for S3toBQ template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting S3 to Bigquery spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {}:{}"
            + "5. {}:{}"
            + "6. {}:{}",
        PROJECT_ID_PROP,
        projectID,
        S3_BQ_INPUT_LOCATION,
        inputFileLocation,
        S3_BQ_OUTPUT_DATASET_NAME,
        bigQueryDataset,
        S3_BQ_OUTPUT_TABLE_NAME,
        bigQueryTable,
        S3_BQ_LD_TEMP_BUCKET_NAME,
        bqTempBucket,
        S3_BQ_INPUT_FORMAT,
        inputFileFormat);
  }
}
