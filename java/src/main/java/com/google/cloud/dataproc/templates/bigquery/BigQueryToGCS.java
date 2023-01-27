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
package com.google.cloud.dataproc.templates.bigquery;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.BQ_GCS_INPUT_TABLE_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.BQ_GCS_OUTPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.BQ_GCS_OUTPUT_FORMAT_AVRO;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.BQ_GCS_OUTPUT_FORMAT_CSV;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.BQ_GCS_OUTPUT_FORMAT_JSON;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.BQ_GCS_OUTPUT_FORMAT_PARQUET;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.BQ_GCS_OUTPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_AVRO_EXTD_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_CSV_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_CSV_HEADER;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_CSV_INFOR_SCHEMA;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPARK_LOG_LEVEL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPARK_READ_FORMAT_BIGQUERY;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryToGCS implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryToGCS.class);

  private final String inputTableName;
  private final String outputFileFormat;
  private final String outputFileLocation;
  private final String sparkLogLevel;

  public BigQueryToGCS() {
    inputTableName = getProperties().getProperty(BQ_GCS_INPUT_TABLE_NAME);
    outputFileFormat = getProperties().getProperty(BQ_GCS_OUTPUT_FORMAT);
    outputFileLocation = getProperties().getProperty(BQ_GCS_OUTPUT_LOCATION);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {

    validateInput();

    SparkSession spark = null;
    try {
      spark = SparkSession.builder().appName("BigQuery to GCS").getOrCreate();

      // Set log level
      spark.sparkContext().setLogLevel(sparkLogLevel);

      Dataset<Row> inputData = spark.read().format(SPARK_READ_FORMAT_BIGQUERY).load(inputTableName);
      DataFrameWriter<Row> writer = inputData.write();
      switch (outputFileFormat) {
        case BQ_GCS_OUTPUT_FORMAT_CSV:
          writer
              .format(GCS_BQ_CSV_FORMAT)
              .option(GCS_BQ_CSV_HEADER, true)
              .option(GCS_BQ_CSV_INFOR_SCHEMA, true)
              .save(outputFileLocation);
          break;
        case BQ_GCS_OUTPUT_FORMAT_JSON:
          writer.json(outputFileLocation);
          break;
        case BQ_GCS_OUTPUT_FORMAT_AVRO:
          writer.format(GCS_BQ_AVRO_EXTD_FORMAT).save(outputFileLocation);
          break;
        case BQ_GCS_OUTPUT_FORMAT_PARQUET:
          writer.parquet(outputFileLocation);
          break;
        default:
          throw new IllegalArgumentException(
              "Currently avro, parquet, csv and json are the only supported formats");
      }
    } catch (Throwable t) {
      LOGGER.error("Exception in BigQueryToGCS", t);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(inputTableName)
        || StringUtils.isAllBlank(outputFileFormat)
        || StringUtils.isAllBlank(outputFileLocation)) {
      LOGGER.error(
          "{},{},{} are required parameter. ",
          BQ_GCS_INPUT_TABLE_NAME,
          BQ_GCS_OUTPUT_FORMAT,
          BQ_GCS_OUTPUT_LOCATION);
      throw new IllegalArgumentException(
          "Required parameters for BigQueryToGCS not passed. "
              + "Set mandatory parameter for BigQueryToGCS template "
              + "in resources/conf/template.properties file.");
    }
  }
}
