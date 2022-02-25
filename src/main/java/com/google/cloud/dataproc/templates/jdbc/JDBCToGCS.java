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
package com.google.cloud.dataproc.templates.jdbc;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToGCS implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(JDBCToGCS.class);

  private String GCSOutputLocation;
  // Choosing default write as overwrite
  private String GCSWriteMode = "OVERWRITE";
  private String GCSPartitionColumn;
  // Default format as csv
  private String GCSOutputFormat = "csv";
  private String jdbcURL;
  private String jdbcDriverClassName;
  private String jdbcInputTable;
  private String jdbcInputDb;
  private String jdbcSQL;
  private String jdbcPropoertiesJSON;

  public JDBCToGCS() {

    GCSOutputLocation = getProperties().getProperty(JDBC_TO_GCS_OUTPUT_LOCATION);
    jdbcInputTable = getProperties().getProperty(JDBC_TO_GCS_INPUT_TABLE_PROP);
    jdbcInputDb = getProperties().getProperty(JDBC_TO_GCS_INPUT_TABLE_DATABASE_PROP);
    jdbcSQL = getProperties().getProperty(JDBC_TO_GCS_SQL);
    jdbcURL = getProperties().getProperty(JDBC_TO_GCS_JDBC_URL);
    jdbcDriverClassName = getProperties().getProperty(JDBC_TO_GCS_JDBC_DRIVER_CLASS_NAME);
    jdbcPropoertiesJSON = getProperties().getProperty(JDBC_TO_GCS_JDBC_PROPERTIES_JSON);
    GCSPartitionColumn = getProperties().getProperty(JDBC_TO_GCS_PARTITION_COLUMN);
    GCSOutputFormat = getProperties().getProperty(JDBC_TO_GCS_OUTPUT_FORMAT);
  }

  @Override
  public void runTemplate() {
    if ((StringUtils.isAllBlank(GCSOutputLocation)
            || StringUtils.isAllBlank(jdbcURL)
            || StringUtils.isAllBlank(jdbcDriverClassName))
        || ((StringUtils.isAllBlank(jdbcInputTable) || StringUtils.isAllBlank(jdbcInputDb))
            || StringUtils.isAllBlank(jdbcSQL))) {
      LOGGER.error(
          "{},{},{} is required parameter. As a source table/sql {},{} or {} are required ",
          JDBC_TO_GCS_OUTPUT_LOCATION,
          JDBC_TO_GCS_JDBC_URL,
          JDBC_TO_GCS_JDBC_DRIVER_CLASS_NAME,
          JDBC_TO_GCS_INPUT_TABLE_DATABASE_PROP,
          JDBC_TO_GCS_INPUT_TABLE_PROP,
          JDBC_TO_GCS_SQL);
      throw new IllegalArgumentException(
          "Required parameters for JDBCToGCS not passed. "
              + "Set mandatory parameter for JDBCToGCS template "
              + "in resources/conf/template.properties file or at runtime. Refer to jdbc/README.md for more instructions.");
    }

    SparkSession spark = null;
    LOGGER.info(
        "Starting JDBC to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5. {},{}"
            + "6. {},{}",
        JDBC_TO_GCS_OUTPUT_LOCATION,
        GCSOutputLocation,
        JDBC_TO_GCS_INPUT_TABLE_PROP,
        jdbcInputTable,
        JDBC_TO_GCS_INPUT_TABLE_DATABASE_PROP,
        jdbcInputDb,
        JDBC_TO_GCS_WRITE_MODE,
        GCSWriteMode,
        JDBC_TO_GCS_JDBC_URL,
        jdbcURL);

    try {
      spark =
          SparkSession.builder().appName("Spark JDBCToGCS Job").enableHiveSupport().getOrCreate();

      Properties connectionProperties = new Properties();
      List<String> jsonList = new ArrayList<>();
      // Following code is to convert string json properties as values.
      // TODO -- Replace deprecated function for reading json
      jsonList.add(jdbcPropoertiesJSON);
      JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
      JavaRDD<String> javaRdd = javaSparkContext.parallelize(jsonList);
      Dataset<Row> jdbcPropertiesDF = spark.read().json(javaRdd);
      String[] propertyNames = jdbcPropertiesDF.schema().fieldNames();
      Row propertyValues = jdbcPropertiesDF.collectAsList().get(0);

      // Adding user provided jdbc properties
      int i = 0;
      for (String propName : propertyNames) {
        connectionProperties.setProperty(propName, propertyValues.getString(i));
        i++;
      }
      connectionProperties.setProperty("driver", jdbcDriverClassName);

      /** Read Input data from JDBC table */
      Dataset<Row> inputData = null;

      // If Input in specified as database and table
      if ((!StringUtils.isAllBlank(jdbcInputTable) && !StringUtils.isAllBlank(jdbcInputDb))) {
        inputData =
            spark.read().jdbc(jdbcURL, jdbcInputDb + "." + jdbcInputTable, connectionProperties);
      }

      // If Input in specified as sql query
      if (!StringUtils.isAllBlank(jdbcSQL)) {

        DataFrameReader jdbcInputConfig =
            spark.read().format("jdbc").option("url", jdbcURL).option("query", jdbcSQL);

        // Adding all custom jdbc properties to config
        for (String propName : connectionProperties.stringPropertyNames()) {
          jdbcInputConfig =
              jdbcInputConfig.option(propName, connectionProperties.getProperty(propName));
        }

        inputData = jdbcInputConfig.load();
      }

      DataFrameWriter<Row> writer = inputData.write().mode(GCSWriteMode).format(GCSOutputFormat);

      /*
       * If optional partition column is passed than partition data by partition
       * column before writing to GCS.
       * */
      if (StringUtils.isNotBlank(GCSPartitionColumn)) {
        LOGGER.info("Partitioning data by :{} cols", GCSPartitionColumn);
        writer = writer.partitionBy(GCSPartitionColumn);
      }

      writer.save(GCSOutputLocation);

    } catch (Throwable th) {
      LOGGER.error("Exception in JDBCtoGCS", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
        throw th;
      }
    }
  }
}
