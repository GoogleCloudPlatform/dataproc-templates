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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_AVRO_EXTD_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_AVRO_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_CSV_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_CSV_HEADER;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_CSV_INFOR_SCHEMA;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_INPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_INPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_LD_TEMP_BUCKET_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_OUTPUT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_OUTPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_PRQT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_TEMP_BUCKET;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_TEMP_QUERY;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_TEMP_TABLE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_OUTPUT_DATASET_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_OUTPUT_TABLE_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.PROJECT_ID_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPARK_LOG_LEVEL;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCStoBigquery implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(GCStoBigquery.class);

  private String projectID;
  private String inputFileLocation;
  private String bigQueryDataset;
  private String bigQueryTable;
  private String inputFileFormat;
  private String bqTempBucket;

  private String bqTempTable;

  private String bqTempQuery;

  private final String sparkLogLevel;

  public GCStoBigquery() {

    projectID = getProperties().getProperty(PROJECT_ID_PROP);
    inputFileLocation = getProperties().getProperty(GCS_BQ_INPUT_LOCATION);
    bigQueryDataset = getProperties().getProperty(GCS_OUTPUT_DATASET_NAME);
    bigQueryTable = getProperties().getProperty(GCS_OUTPUT_TABLE_NAME);
    inputFileFormat = getProperties().getProperty(GCS_BQ_INPUT_FORMAT);
    bqTempBucket = getProperties().getProperty(GCS_BQ_LD_TEMP_BUCKET_NAME);
    bqTempTable = getProperties().getProperty(GCS_BQ_TEMP_TABLE);
    bqTempQuery = getProperties().getProperty(GCS_BQ_TEMP_QUERY);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {
    validateInput();

    SparkSession spark = null;
    LOGGER.info("input format: {}", inputFileFormat);

    try {
      spark = SparkSession.builder().appName("GCS to Bigquery load").getOrCreate();

      // Set log level
      spark.sparkContext().setLogLevel(sparkLogLevel);

      Dataset<Row> inputData = null;

      switch (inputFileFormat) {
        case GCS_BQ_CSV_FORMAT:
          inputData =
              spark
                  .read()
                  .format(GCS_BQ_CSV_FORMAT)
                  .option(GCS_BQ_CSV_HEADER, true)
                  .option(GCS_BQ_CSV_INFOR_SCHEMA, true)
                  .load(inputFileLocation);
          break;
        case GCS_BQ_AVRO_FORMAT:
          inputData = spark.read().format(GCS_BQ_AVRO_EXTD_FORMAT).load(inputFileLocation);
          break;
        case GCS_BQ_PRQT_FORMAT:
          inputData = spark.read().parquet(inputFileLocation);
          break;
        default:
          throw new IllegalArgumentException(
              "Currently avro, parquet and csv are the only supported formats");
      }

      if (bqTempTable != null && bqTempQuery != null) {
        inputData.createOrReplaceGlobalTempView(bqTempTable);
        inputData = spark.sql(bqTempQuery);
      }

      inputData
          .write()
          .format(GCS_BQ_OUTPUT_FORMAT)
          .option(GCS_BQ_CSV_HEADER, true)
          .option(GCS_BQ_OUTPUT, bigQueryDataset + "." + bigQueryTable)
          .option(GCS_BQ_TEMP_BUCKET, bqTempBucket)
          .mode(SaveMode.Append)
          .save();

    } catch (Throwable th) {
      LOGGER.error("Exception in GCStoBigquery", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(projectID)
        || StringUtils.isAllBlank(inputFileLocation)
        || StringUtils.isAllBlank(bigQueryDataset)
        || StringUtils.isAllBlank(bigQueryTable)
        || StringUtils.isAllBlank(inputFileFormat)
        || StringUtils.isAllBlank(bqTempBucket)) {
      LOGGER.error(
          "{},{},{},{},{},{} are required parameter. ",
          PROJECT_ID_PROP,
          GCS_BQ_INPUT_LOCATION,
          GCS_OUTPUT_DATASET_NAME,
          GCS_OUTPUT_TABLE_NAME,
          GCS_BQ_INPUT_FORMAT,
          GCS_BQ_LD_TEMP_BUCKET_NAME);
      throw new IllegalArgumentException(
          "Required parameters for GCStoBQ not passed. "
              + "Set mandatory parameter for GCStoBQ template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting GCS to Bigquery spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {}:{}"
            + "5. {}:{}",
        GCS_BQ_INPUT_LOCATION,
        inputFileLocation,
        GCS_OUTPUT_DATASET_NAME,
        bigQueryDataset,
        GCS_OUTPUT_TABLE_NAME,
        bigQueryTable,
        GCS_BQ_INPUT_FORMAT,
        inputFileFormat,
        GCS_BQ_LD_TEMP_BUCKET_NAME,
        bqTempBucket);
  }
}
