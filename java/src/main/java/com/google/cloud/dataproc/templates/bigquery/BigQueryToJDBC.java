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
package com.google.cloud.dataproc.templates.bigquery;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryToJDBC implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryToJDBC.class);

  private final String inputTableName;
  private final String outputTableName;
  private final String outputJDBCURL;
  private final String outputBatchSize;
  private final String outputJDBCDriver;
  private final String outputSaveMode;
  private final String sparkLogLevel;

  public BigQueryToJDBC() {
    inputTableName = getProperties().getProperty(BQ_JDBC_INPUT_TABLE_NAME);
    outputJDBCURL = getProperties().getProperty(BQ_JDBC_OUTPUT_URL);
    outputBatchSize = getProperties().getProperty(BQ_JDBC_OUTPUT_BATCH_SIZE);
    outputJDBCDriver = getProperties().getProperty(BQ_JDBC_OUTPUT_DRIVER);
    outputTableName = getProperties().getProperty(BQ_JDBC_OUTPUT_TABLE_NAME);
    outputSaveMode = getProperties().getProperty(BQ_JDBC_OUTPUT_MODE);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {

    // Create a SparkConf object and set configurations
    SparkConf conf =
        new SparkConf()
            .setAppName("BigQuery to JDBC")
            .set("spark.sql.viewsEnabled", "true")
            .set("spark.sql.materializationDataset", "<dataset>");

    // Initialize SparkSession using SparkConf
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

    // Set Spark properties for performance optimization
    spark.sparkContext().setLogLevel(sparkLogLevel);

    Dataset<Row> inputData = spark.read().format(SPARK_READ_FORMAT_BIGQUERY).load(inputTableName);
    write(inputData);
  }

  public void write(Dataset<Row> dataset) {
    dataset
        .write()
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL(), outputJDBCURL)
        .option(JDBCOptions.JDBC_TABLE_NAME(), outputTableName)
        .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), outputBatchSize)
        .option(JDBCOptions.JDBC_DRIVER_CLASS(), outputJDBCDriver)
        .mode(outputSaveMode)
        .save();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(inputTableName)
        || StringUtils.isAllBlank(outputJDBCURL)
        || StringUtils.isAllBlank(outputJDBCDriver)
        || StringUtils.isAllBlank(outputTableName)) {
      LOGGER.error(
          "{},{},{},{} are required parameter. ",
          BQ_JDBC_INPUT_TABLE_NAME,
          BQ_JDBC_OUTPUT_URL,
          BQ_JDBC_OUTPUT_DRIVER,
          BQ_JDBC_OUTPUT_TABLE_NAME);
      throw new IllegalArgumentException(
          "Required parameters for BigQueryToJDBC not passed. "
              + "Set mandatory parameter for BigQueryToJDBC template "
              + "in resources/conf/template.properties file.");
    }
  }
}
