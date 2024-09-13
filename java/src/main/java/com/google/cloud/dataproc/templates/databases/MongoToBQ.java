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
package com.google.cloud.dataproc.templates.databases;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoToBQ implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(MongoToBQ.class);
  private final MongoToBQConfig config;

  public MongoToBQ(MongoToBQConfig config) {

    this.config = config;
  }

  public static MongoToBQ of(String... args) {
    MongoToBQConfig mongoToBQConfig = MongoToBQConfig.fromProperties(PropertyUtil.getProperties());
    LOGGER.info("Config loaded\n{}", mongoToBQConfig);
    return new MongoToBQ(mongoToBQConfig);
  }

  @Override
  public void validateInput() throws IllegalArgumentException {
    ValidationUtil.validateOrThrow(config);
  }

  @Override
  public void runTemplate()
      throws StreamingQueryException, TimeoutException, SQLException, InterruptedException {

    LOGGER.info("Initialize the Spark session");
    SparkSession spark = SparkSession.builder().appName("Spark MongoToBQ Job").getOrCreate();

    LOGGER.info("Read from Mongo");
    Dataset<Row> rowDataset =
        spark
            .read()
            .format(MONGO_FORMAT)
            .option(MONGO_INPUT_URI, config.getInputURI())
            .option(MONGO_DATABASE, config.getInputDatabase())
            .option(MONGO_COLLECTION, config.getInputCollection())
            .load();

    LOGGER.info("Write to BigQuery");
    rowDataset
        .write()
        .format(MONGO_BQ_FORMAT)
        .option(
            MONGO_BQ_TABLE,
            String.format(
                "%s.%s.%s",
                config.getProjectId(), config.getBqOutputDataset(), config.getBqOutputTable()))
        .option(MONGO_BQ_TEMP_BUCKET, config.getBqTempBucket())
        .mode(config.getOutputMode().toLowerCase())
        .save();
  }
}
