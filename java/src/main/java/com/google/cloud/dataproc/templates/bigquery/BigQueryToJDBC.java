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
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryToJDBC implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryToJDBC.class);
  private final BigQueryToJDBCConfig config;

  public BigQueryToJDBC(BigQueryToJDBCConfig config) {
    this.config = config;
  }

  public static BigQueryToJDBC of(String... args) {
    BigQueryToJDBCConfig config = BigQueryToJDBCConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new BigQueryToJDBC(config);
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
    spark.sparkContext().setLogLevel(config.getSparklogLevel());

    Dataset<Row> inputData =
        spark.read().format(SPARK_READ_FORMAT_BIGQUERY).load(config.getInputTableName());
    write(inputData);
  }

  public void write(Dataset<Row> dataset) {
    dataset
        .write()
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL(), config.getOutputJDBCURL())
        .option(JDBCOptions.JDBC_TABLE_NAME(), config.getOutputTableName())
        .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), config.getOutputBatchSize())
        .option(JDBCOptions.JDBC_DRIVER_CLASS(), config.getOutputJDBCDriver())
        .mode(config.getOutputSaveMode())
        .save();
  }

  public void validateInput() {}
}
