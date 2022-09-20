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

  private String tempTable;
  private String tempQuery;

  public SpannerToGCS() {
    projectId = getProperties().getProperty(PROJECT_ID_PROP);
    instanceId = getProperties().getProperty(SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID);
    databaseId = getProperties().getProperty(SPANNER_GCS_INPUT_DATABASE_ID);
    tableId = getProperties().getProperty(SPANNER_GCS_INPUT_TABLE_ID);
    gcsWritePath = getProperties().getProperty(SPANNER_GCS_OUTPUT_GCS_PATH);
    gcsSaveMode = getProperties().getProperty(SPANNER_GCS_OUTPUT_GCS_SAVEMODE);
    outputFormat = getProperties().getProperty(SPANNER_GCS_OUTPUT_FORMAT);
    tempTable = getProperties().getProperty(SPANNER_GCS_TEMP_TABLE);
    tempQuery = getProperties().getProperty(SPANNER_GCS_TEMP_QUERY);
  }

  @Override
  public void runTemplate() {
	validateInput();
    String spannerUrl =
        String.format(
            "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s?lenient=true",
            projectId, instanceId, databaseId);
    JdbcDialects.registerDialect(new SpannerJdbcDialect());

    LOGGER.info("Spanner URL: " + spannerUrl);

    SparkSession spark = SparkSession.builder().appName("DatabaseToGCS Dataproc job").getOrCreate();

   // LOGGER.debug("added jars : {}", spark.sparkContext().addedJars().keys());
    Dataset<Row> jdbcDF =
        spark
            .read()
            .format("jdbc")
            .option(JDBCOptions.JDBC_URL(), spannerUrl)
            .option(JDBCOptions.JDBC_TABLE_NAME(), tableId)
            .option(JDBCOptions.JDBC_DRIVER_CLASS(), SPANNER_JDBC_DRIVER)
            .load();

    LOGGER.info("Data load complete from table/query: " + tableId);

    if (StringUtils.isNotBlank(tempTable) && StringUtils.isNotBlank(tempQuery)) {
      jdbcDF.createOrReplaceGlobalTempView(tempTable);
      jdbcDF = spark.sql(tempQuery);
    }

    jdbcDF.write().format(outputFormat).mode(gcsSaveMode).save(gcsWritePath);

    spark.stop();
  }

 void validateInput() {
	   if (StringUtils.isAllBlank(projectId)
		        || StringUtils.isAllBlank(instanceId)
		        || StringUtils.isAllBlank(databaseId)
		        || StringUtils.isAllBlank(tableId)
		        || StringUtils.isAllBlank(gcsWritePath)
		        || StringUtils.isAllBlank(gcsSaveMode)
		        || StringUtils.isAllBlank(outputFormat)) {
		      LOGGER.error(
		          "{},{},{},{},{},{},{} are required parameters. ",
		          PROJECT_ID_PROP,
		          SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID,
		          SPANNER_GCS_INPUT_DATABASE_ID,
		          SPANNER_GCS_INPUT_TABLE_ID,
		          SPANNER_GCS_OUTPUT_GCS_PATH,
		          SPANNER_GCS_OUTPUT_GCS_SAVEMODE,
		          SPANNER_GCS_OUTPUT_FORMAT);
		      throw new IllegalArgumentException(
		          "Required parameters for SpannerToGCS not passed. "
		              + "Set mandatory parameter for SpannerToGCS template "
		              + "in resources/conf/template.properties file or at runtime. Refer to d/README.md for more instructions.");
		    }
	
}
}
