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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCStoMongo implements BaseTemplate {
  public static final Logger LOGGER = LoggerFactory.getLogger(GCStoMongo.class);

  private final String inputFileLocation;
  private final String inputFileFormat;
  private final String sparkLogLevel;
  private final String mongoUrl;
  private final String mongoDatabase;
  private final String mongoCollection;
  private String mongoBatchSize;
  private String mongoSaveMode;

  public GCStoMongo() {

    inputFileLocation = getProperties().getProperty(GCS_MONGO_INPUT_LOCATION);
    inputFileFormat = getProperties().getProperty(GCS_MONGO_INPUT_FORMAT);
    mongoUrl = getProperties().getProperty(GCS_MONGO_URL);
    mongoDatabase = getProperties().getProperty(GCS_MONGO_DATABASE);
    mongoCollection = getProperties().getProperty(GCS_MONGO_COLLECTION);
    mongoBatchSize = getProperties().getProperty(GCS_MONGO_BATCH_SIZE);
    mongoSaveMode = getProperties().getProperty(GCS_MONGO_OUTPUT_MODE);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {

    SparkSession spark = null;

    spark = SparkSession.builder().appName("GCS to Mongo load").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    Dataset<Row> inputData = null;
    switch (inputFileFormat) {
      case SPARK_FILE_FORMAT_CSV:
        inputData =
            spark
                .read()
                .format(inputFileFormat)
                .option(SPARK_CSV_HEADER, true)
                .option(SPARK_INFER_SCHEMA, true)
                .load(inputFileLocation);
        break;
      case SPARK_FILE_FORMAT_PARQUET:
        inputData = spark.read().parquet(inputFileLocation);
        break;
      case SPARK_FILE_FORMAT_AVRO:
        inputData = spark.read().format(inputFileFormat).load(inputFileLocation);
        break;
      case SPARK_FILE_FORMAT_JSON:
        inputData = spark.read().json(inputFileLocation);
        break;
      default:
        throw new IllegalArgumentException(
            "Currently avro, parquet, csv and json are the only supported formats");
    }
    inputData
        .write()
        .format(MONGO_FORMAT)
        .option(MONGO_URL, mongoUrl)
        .option(MONGO_DATABASE, mongoDatabase)
        .option(MONGO_COLLECTION, mongoCollection)
        .option(MONGO_BATCH_SIZE, mongoBatchSize)
        .mode(mongoSaveMode)
        .save();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(inputFileLocation)
        || StringUtils.isAllBlank(inputFileFormat)
        || StringUtils.isAllBlank(mongoUrl)
        || StringUtils.isAllBlank(mongoDatabase)
        || StringUtils.isAllBlank(mongoCollection)) {
      LOGGER.error(
          "{},{},{},{},{} are required parameters. ",
          GCS_MONGO_INPUT_LOCATION,
          GCS_MONGO_INPUT_FORMAT,
          GCS_MONGO_URL,
          GCS_MONGO_DATABASE,
          GCS_MONGO_COLLECTION);
      throw new IllegalArgumentException(
          "Required parameters for GCStoMongo not passed. "
              + "Set mandatory parameter for GCStoMongo template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting GCS to Mongo spark job with following parameters:"
            + "1. {}:{} | "
            + "2. {}:{} | "
            + "3. {}:{} | "
            + "4. {}:{} | "
            + "5. {}:{} | ",
        GCS_MONGO_INPUT_LOCATION,
        inputFileLocation,
        GCS_MONGO_INPUT_FORMAT,
        inputFileFormat,
        GCS_MONGO_URL,
        mongoUrl,
        GCS_MONGO_DATABASE,
        mongoDatabase,
        GCS_MONGO_COLLECTION,
        mongoCollection);
  }
}
