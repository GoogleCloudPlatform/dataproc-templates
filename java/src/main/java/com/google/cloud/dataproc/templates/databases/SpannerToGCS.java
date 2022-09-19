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
package com.google.cloud.dataproc.templates.databases;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.dialects.SpannerJdbcDialect;
import com.google.cloud.dataproc.templates.BaseTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerToGCS implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerToGCS.class);
  public static final String SPANNER_JDBC_DRIVER = "com.google.cloud.spanner.jdbc.JdbcDriver";

  private final String projectId;
  private final String instanceId;
  private final String databaseId;
  private final String tableId;
  private final String gcsWritePath;
  private final String gcsSaveMode;
  private final String outputFormat;
  private final String partitionColumn;

  public SpannerToGCS() {
    projectId = getProperties().getProperty(PROJECT_ID_PROP);
    instanceId = getProperties().getProperty(SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID);
    databaseId = getProperties().getProperty(SPANNER_GCS_INPUT_DATABASE_ID);
    tableId = getProperties().getProperty(SPANNER_GCS_INPUT_TABLE_ID);
    gcsWritePath = getProperties().getProperty(SPANNER_GCS_OUTPUT_GCS_PATH);
    gcsSaveMode = getProperties().getProperty(SPANNER_GCS_OUTPUT_GCS_SAVEMODE);
    outputFormat = getProperties().getProperty(SPANNER_GCS_OUTPUT_FORMAT);
    partitionColumn = getProperties().getProperty(SPANNER_GCS_OUTPUT_PARTITION_COLUMN);
  }

  @Override
  public void runTemplate() {
    String spannerUrl =
        String.format(
            "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s?lenient=true",
            projectId, instanceId, databaseId);
    JdbcDialects.registerDialect(new SpannerJdbcDialect());

    LOGGER.info("Spanner URL: " + spannerUrl);

    SparkSession spark = SparkSession.builder().appName("DatabaseToGCS Dataproc job").getOrCreate();

    LOGGER.debug("added jars : {}", spark.sparkContext().addedJars().keys());
    Dataset<Row> jdbcDF =
        spark
            .read()
            .format("jdbc")
            .option(JDBCOptions.JDBC_URL(), spannerUrl)
            .option(JDBCOptions.JDBC_TABLE_NAME(), tableId)
            .option(JDBCOptions.JDBC_DRIVER_CLASS(), SPANNER_JDBC_DRIVER)
            .load();

    LOGGER.info("Data load complete from table/query: " + tableId);

    DataFrameWriter<Row> writer = jdbcDF.write().format(outputFormat).mode(gcsSaveMode);

    if (StringUtils.isNotBlank(partitionColumn)) {
      LOGGER.info("Partitioning data by :{} cols", partitionColumn);
      writer = writer.partitionBy(partitionColumn);
    }

    writer.save(gcsWritePath);

    spark.stop();
  }
}
