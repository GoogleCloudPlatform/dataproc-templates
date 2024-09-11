/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.dataproc.templates.mongo;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoToBQ implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(MongoToBQ.class);

  private final String projectId;
  private final String inputURI;
  private final String inputDatabase;
  private final String inputCollection;
  private final String outputMode;
  private final String bqOutputDataset;
  private final String bqOutputTable;
  private final String bqTempBucket;

  public MongoToBQ() {

    projectId = getProperties().getProperty(PROJECT_ID_PROP);
    inputURI = getProperties().getProperty(MONGO_BQ_INPUT_URI);
    inputDatabase = getProperties().getProperty(MONGO_BQ_INPUT_DATABASE);
    inputCollection = getProperties().getProperty(MONGO_BQ_INPUT_COLLECTION);
    outputMode = getProperties().getProperty(MONGO_BQ_OUTPUT_MODE);
    bqOutputDataset = getProperties().getProperty(MONGO_BQ_OUTPUT_DATASET);
    bqOutputTable = getProperties().getProperty(MONGO_BQ_OUTPUT_TABLE);
    bqTempBucket = getProperties().getProperty(MONGO_BQ_TEMP_BUCKET_NAME);
  }

  @Override
  public void validateInput() throws IllegalArgumentException {

    if (StringUtils.isBlank(inputURI)
        || StringUtils.isBlank(inputDatabase)
        || StringUtils.isBlank(inputCollection)
        || StringUtils.isBlank(outputMode)
        || StringUtils.isBlank(bqOutputDataset)
        || StringUtils.isBlank(bqOutputTable)
        || StringUtils.isBlank(bqTempBucket)) {

      LOGGER.error(
          "{},{},{},{},{},{},{} is required parameter. ",
          MONGO_BQ_INPUT_URI,
          MONGO_BQ_INPUT_DATABASE,
          MONGO_BQ_INPUT_COLLECTION,
          MONGO_BQ_OUTPUT_MODE,
          MONGO_BQ_OUTPUT_DATASET,
          MONGO_BQ_OUTPUT_TABLE,
          MONGO_BQ_TEMP_BUCKET_NAME);
      throw new IllegalArgumentException(
          "Required parameters for MongoToBQ not passed. "
              + "Set mandatory parameter for MongoToBQ template "
              + "in resources/conf/template.properties file.");
    }
  }

  @Override
  public void runTemplate()
      throws StreamingQueryException, TimeoutException, SQLException, InterruptedException {

    // Initialize the Spark session
    SparkSession spark = SparkSession.builder().appName("Spark MongoToBQ Job").getOrCreate();

    // Read from Mongo
    Dataset<Row> rowDataset =
        spark
            .read()
            .format(MONGO_FORMAT)
            .option(MONGO_INPUT_URI, inputURI)
            .option(MONGO_DATABASE, inputDatabase)
            .option(MONGO_COLLECTION, inputCollection)
            .load();

    // Write to BigQuery
    rowDataset
        .write()
        .format(MONGO_BQ_FORMAT)
        .option(
            MONGO_BQ_TABLE, String.format("%s.%s.%s", projectId, bqOutputDataset, bqOutputTable))
        .option(MONGO_BQ_TEMP_BUCKET, bqTempBucket)
        .mode(outputMode.toLowerCase())
        .save();
  }
}
