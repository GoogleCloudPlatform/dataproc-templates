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
package com.google.cloud.dataproc.templates.databases;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_INPUT_COLLECTION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_INPUT_DATABASE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_INPUT_URI;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_OUTPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_OUTPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_OUTPUT_MODE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPARK_LOG_LEVEL;

import com.google.cloud.dataproc.templates.BaseTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoToGCS implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoToGCS.class);

  private final String inputCollection;
  private final String outputFileFormat;
  private final String outputFileLocation;
  private final String outputMode;
  private final String inputUri;
  private final String inputDatabase;
  private final String sparkLogLevel;

  public MongoToGCS() {
    inputCollection = getProperties().getProperty(MONGO_GCS_INPUT_COLLECTION);
    outputFileFormat = getProperties().getProperty(MONGO_GCS_OUTPUT_FORMAT);
    outputFileLocation = getProperties().getProperty(MONGO_GCS_OUTPUT_LOCATION);
    outputMode = getProperties().getProperty(MONGO_GCS_OUTPUT_MODE);
    inputUri = getProperties().getProperty(MONGO_GCS_INPUT_URI);
    inputDatabase = getProperties().getProperty(MONGO_GCS_INPUT_DATABASE);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {

    // Initialize Spark Session

    SparkSession spark = null;
    spark = SparkSession.builder().appName("Mongo To GCS").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    // Read Data
    Dataset<Row> inputData =
        spark
            .read()
            .format("com.mongodb.spark.sql.DefaultSource")
            .option("spark.mongodb.input.uri", inputUri)
            .option("spark.mongodb.input.database", inputDatabase)
            .option("spark.mongodb.input.collection", inputCollection)
            .load();

    // Write Data
    inputData.write().format(outputFileFormat).mode(outputMode).save(outputFileLocation);
  }

  @Override
  public void validateInput() {
    if (StringUtils.isAllBlank(inputCollection)
        || StringUtils.isAllBlank(outputFileFormat)
        || StringUtils.isAllBlank(outputFileLocation)
        || StringUtils.isAllBlank(inputUri)
        || StringUtils.isAllBlank(inputDatabase)) {
      LOGGER.error(
          "{},{},{},{},{} are required parameter. ",
          MONGO_GCS_INPUT_COLLECTION,
          MONGO_GCS_OUTPUT_FORMAT,
          MONGO_GCS_OUTPUT_LOCATION,
          MONGO_GCS_INPUT_URI,
          MONGO_GCS_INPUT_DATABASE);
      throw new IllegalArgumentException(
          "Required parameters for Mongo to GCS not passed. "
              + "Set mandatory parameter for Mongo to GCS template "
              + "in resources/conf/template.properties file.");
    }
  }
}
