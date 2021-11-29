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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_INPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_INPUT_FORMAT_AVRO;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_INPUT_FORMAT_AVRO_EXTD;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_INPUT_FORMAT_PARQUET;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_INPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_OUTPUT_DATABASE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_OUTPUT_INSTANCE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_OUTPUT_PRIMARY_KEY;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_OUTPUT_SAVE_MODE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_OUTPUT_TABLE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.PROJECT_ID_PROP;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.spanner.jdbc.SpannerJdbcDialect;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSToSpanner implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSToSpanner.class);

  private final String inputLocation;
  private final String inputFormat;
  private final String projectId;
  private final String instance;
  private final String database;
  private final String table;
  private final String saveModeString;
  private final String primaryKey;

  public GCSToSpanner() {
    inputFormat = getProperties().getProperty(GCS_SPANNER_INPUT_FORMAT);
    inputLocation = getProperties().getProperty(GCS_SPANNER_INPUT_LOCATION);
    projectId = getProperties().getProperty(PROJECT_ID_PROP);
    instance = getProperties().getProperty(GCS_SPANNER_OUTPUT_INSTANCE);
    database = getProperties().getProperty(GCS_SPANNER_OUTPUT_DATABASE);
    table = getProperties().getProperty(GCS_SPANNER_OUTPUT_TABLE);
    saveModeString =
        getProperties()
            .getProperty(GCS_SPANNER_OUTPUT_SAVE_MODE, SaveMode.ErrorIfExists.toString());
    primaryKey = getProperties().getProperty(GCS_SPANNER_OUTPUT_PRIMARY_KEY);
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(projectId)
        || StringUtils.isAllBlank(instance)
        || StringUtils.isAllBlank(database)
        || StringUtils.isAllBlank(table)
        || StringUtils.isAllBlank(inputLocation)
        || StringUtils.isAllBlank(inputFormat)) {
      LOGGER.error(
          "{},{},{},{},{},{} are required parameter. ",
          PROJECT_ID_PROP,
          GCS_SPANNER_OUTPUT_INSTANCE,
          GCS_SPANNER_OUTPUT_DATABASE,
          GCS_SPANNER_OUTPUT_TABLE,
          GCS_SPANNER_INPUT_LOCATION,
          GCS_SPANNER_INPUT_FORMAT);
      throw new IllegalArgumentException(
          "Required parameters for GCSToSpanner not passed. "
              + "Set mandatory parameter for GCSToSpanner template "
              + "in resources/conf/template.properties file.");
    }
    SaveMode saveMode = SaveMode.valueOf(saveModeString);
    switch (saveMode) {
      case Overwrite:
      case ErrorIfExists:
        if (StringUtils.isAllBlank(primaryKey)) {
          throw new IllegalArgumentException(
              String.format(
                  "Parameter %s is required if %s is ErrorIfExists or Overwrite",
                  GCS_SPANNER_OUTPUT_PRIMARY_KEY, GCS_SPANNER_OUTPUT_SAVE_MODE));
        }
    }

    LOGGER.info(
        "Starting GCS to Spanner spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {}:{}"
            + "5. {}:{}"
            + "6. {}:{}"
            + "7. {}:{}"
            + "8. {}:{}",
        PROJECT_ID_PROP,
        projectId,
        GCS_SPANNER_OUTPUT_INSTANCE,
        instance,
        GCS_SPANNER_OUTPUT_DATABASE,
        database,
        GCS_SPANNER_OUTPUT_TABLE,
        database,
        GCS_SPANNER_INPUT_LOCATION,
        inputLocation,
        GCS_SPANNER_INPUT_FORMAT,
        inputFormat,
        GCS_SPANNER_OUTPUT_SAVE_MODE,
        saveModeString,
        GCS_SPANNER_OUTPUT_PRIMARY_KEY,
        primaryKey);

    String spannerUrl =
        String.format(
            "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s?lenient=true",
            projectId, instance, database);
    JdbcDialects.registerDialect(new SpannerJdbcDialect());

    SparkSession spark = null;
    try {
      spark = SparkSession.builder().appName("GCS to Spanner").getOrCreate();

      Dataset<Row> inputDataset;

      switch (inputFormat) {
        case GCS_SPANNER_INPUT_FORMAT_AVRO:
          inputDataset =
              spark.read().format(GCS_SPANNER_INPUT_FORMAT_AVRO_EXTD).load(inputLocation);
          break;
        case GCS_SPANNER_INPUT_FORMAT_PARQUET:
          inputDataset = spark.read().parquet(inputLocation);
          break;
        default:
          throw new IllegalArgumentException(
              "Currently avro and parquet are the only supported formats");
      }

      // For options see
      // https://spark.apache.org/docs/3.1.2/sql-data-sources-jdbc.html
      inputDataset
          .write()
          .format("jdbc")
          .option(JDBCOptions.JDBC_URL(), spannerUrl)
          .option(JDBCOptions.JDBC_TABLE_NAME(), table)
          .option(
              JDBCOptions.JDBC_CREATE_TABLE_OPTIONS(),
              String.format("PRIMARY KEY (%s)", primaryKey))
          // numPartitions
          .option(
              JDBCOptions.JDBC_TXN_ISOLATION_LEVEL(),
              "NONE") // Needed because transaction have a 20,000 mutation limit per commit.
          .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), 1000) // default is 1000
          .option(JDBCOptions.JDBC_DRIVER_CLASS(), "com.google.cloud.spanner.jdbc.JdbcDriver")
          .mode(saveMode)
          .save();
    } catch (Throwable t) {
      LOGGER.error("Exception in GCSToSpanner", t);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
