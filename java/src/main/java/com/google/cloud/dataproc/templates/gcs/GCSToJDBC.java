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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_JDBC_AVRO_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_JDBC_CSV_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_JDBC_CSV_HEADER;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_JDBC_CSV_INFER_SCHEMA;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_JDBC_ORC_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_JDBC_PRQT_FORMAT;

import com.google.cloud.dataproc.dialects.SpannerJdbcDialect;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSToJDBC implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSToJDBC.class);
  private final GCSToJDBCConfig config;

  public GCSToJDBC(GCSToJDBCConfig config) {
    this.config = config;
  }

  public static GCSToJDBC of(String... args) {
    GCSToJDBCConfig config = GCSToJDBCConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new GCSToJDBC(config);
  }

  @Override
  public void runTemplate() {
    validateInput();
    try (SparkSession spark = SparkSession.builder().appName("GCS to JDBC").getOrCreate()) {
      // Set log level
      spark.sparkContext().setLogLevel(config.getSparkLogLevel());

      Dataset<Row> dataset = load(spark);

      write(dataset);
    }
  }

  public Dataset<Row> load(SparkSession spark) {
    Dataset<Row> inputData = null;

    switch (config.getInputFormat()) {
      case GCS_JDBC_CSV_FORMAT:
        inputData =
            spark
                .read()
                .format(config.getInputFormat())
                .option(GCS_JDBC_CSV_HEADER, true)
                .option(GCS_JDBC_CSV_INFER_SCHEMA, true)
                .load(config.getInputLocation());
        break;
      case GCS_JDBC_AVRO_FORMAT:
      case GCS_JDBC_ORC_FORMAT:
        inputData = spark.read().format(config.getInputFormat()).load(config.getInputLocation());
        break;
      case GCS_JDBC_PRQT_FORMAT:
        inputData = spark.read().parquet(config.getInputLocation());
        break;
      default:
        throw new IllegalArgumentException(
            "Currently avro, orc, parquet and csv are the only supported formats");
    }

    return inputData;
  }

  public void write(Dataset<Row> dataset) {
    JdbcDialects.registerDialect(new SpannerJdbcDialect());

    int current_partitions = dataset.toJavaRDD().getNumPartitions();
    if (config.getCustomSparkPartitions() == null || config.getCustomSparkPartitions().isEmpty()) {
      int custom_partitions = current_partitions;
    } else {
      int custom_partitions = Integer.parseInt(config.getCustomSparkPartitions());
      LOGGER.info("Current Spark partitions: ", Integer.toString(current_partitions));

      if (current_partitions > custom_partitions) {
        LOGGER.info(
            "Current partitions are greater than provided custom partitions. Will use coalesce()... ");
        dataset = dataset.coalesce(custom_partitions);
      } else if (current_partitions < custom_partitions) {
        LOGGER.info(
            "Current partitions are smaller than provided custom partitions. Will use repartition()... ");
        dataset = dataset.repartition(custom_partitions);
      } else {
        LOGGER.info("Current partitions are equal to provided custom partitions. ");
      }
    }

    dataset
        .write()
        .format("jdbc")
        .option(JDBCOptions.JDBC_URL(), config.getJDBCUrl())
        .option(JDBCOptions.JDBC_TABLE_NAME(), config.getTable())
        .option(
            JDBCOptions.JDBC_BATCH_INSERT_SIZE(), config.getBatchInsertSize()) // default is 1000
        .option(JDBCOptions.JDBC_DRIVER_CLASS(), config.getJDBCDriver())
        .mode(config.getSaveMode())
        .save();
  }

  public void validateInput() {}
}
