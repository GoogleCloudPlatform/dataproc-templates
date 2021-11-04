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

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerToGCS implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerToGCS.class);

  private final String projectId;
  private final String instanceId;
  private final String databaseId;
  private final String tableId;
  private final String gcsWritePath;

  public SpannerToGCS() {
    projectId = getProperties().getProperty(PROJECT_ID_PROP);
    instanceId = getProperties().getProperty(SPANNER_INSTANCE_ID_PROP);
    databaseId = getProperties().getProperty(SPANNER_DATABASE_ID_PROP);
    tableId = getProperties().getProperty(SPANNER_TABLE_ID_PROP);
    gcsWritePath = getProperties().getProperty(SPANNER_GCS_PATH);
  }

  @Override
  public void runTemplate() {

    SparkSession spark = null;
    try {

      String spannerUrl =
          String.format(
              "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s?lenient=true",
              projectId, instanceId, databaseId);

      LOGGER.info("Spanner URL: " + spannerUrl);

      spark = SparkSession.builder().appName("DatabaseToGCS Dataproc job").getOrCreate();

      LOGGER.debug("added jars : {}", spark.sparkContext().addedJars().keys());
      spark.sparkContext().hadoopConfiguration().getClassLoader();

      Dataset<Row> jdbcDF =
          spark
              .read()
              .format("jdbc")
              .option(JDBCOptions.JDBC_URL(), spannerUrl)
              .option(JDBCOptions.JDBC_TABLE_NAME(), tableId)
              .option(JDBCOptions.JDBC_DRIVER_CLASS(), "com.google.cloud.spanner.jdbc.JdbcDriver")
              .load();

      LOGGER.info("Data load complete from table/query: " + tableId);
      jdbcDF.write().format("avro").mode(SaveMode.ErrorIfExists).save(gcsWritePath);

      spark.stop();
    } catch (Throwable th) {
      LOGGER.error("Exception in DatabaseToGCS", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
      throw th;
    }
  }
}
