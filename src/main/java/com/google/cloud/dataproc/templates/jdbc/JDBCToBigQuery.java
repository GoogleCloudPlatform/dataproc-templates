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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToBigQuery implements BaseTemplate {

  public static final Logger LOGGER =
      LoggerFactory.getLogger(com.google.cloud.dataproc.templates.jdbc.JDBCToBigQuery.class);

  private String bqLocation;
  private String warehouseLocation;
  private String jdbcURL;
  private String jdbcDriverClassName;
  private String jdbcInputTable;
  private String jdbcInputDb;
  private String bqAppendMode;
  private String jdbcPropoertiesJSON;

  public JDBCToBigQuery() {

    bqLocation = getProperties().getProperty(JDBC_TO_BQ_BIGQUERY_LOCATION);
    warehouseLocation = getProperties().getProperty(JDBC_TO_BQ_WAREHOUSE_LOCATION_PROP);
    jdbcInputTable = getProperties().getProperty(JDBC_TO_BQ_INPUT_TABLE_PROP);
    jdbcInputDb = getProperties().getProperty(JDBC_TO_BQ_INPUT_TABLE_DATABASE_PROP);
    bqAppendMode = getProperties().getProperty(JDBC_TO_BQ_APPEND_MODE);
    jdbcURL = getProperties().getProperty(JDBC_TO_BQ_JDBC_URL);
    jdbcDriverClassName = getProperties().getProperty(JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME);
    jdbcPropoertiesJSON = getProperties().getProperty(JDBC_TO_BQ_JDBC_PROPERTIES_JSON);
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(bqLocation)
        || StringUtils.isAllBlank(jdbcInputTable)
        || StringUtils.isAllBlank(warehouseLocation)
        || StringUtils.isAllBlank(jdbcInputDb)) {
      LOGGER.error(
          "{},{},{},{} is required parameter. ",
          JDBC_TO_BQ_BIGQUERY_LOCATION,
          JDBC_TO_BQ_INPUT_TABLE_PROP,
          JDBC_TO_BQ_INPUT_TABLE_DATABASE_PROP,
          JDBC_TO_BQ_WAREHOUSE_LOCATION_PROP);
      throw new IllegalArgumentException(
          "Required parameters for JDBCToBigQuery not passed. "
              + "Set mandatory parameter for JDBCToBigQuery template "
              + "in resources/conf/template.properties file.");
    }

    SparkSession spark = null;
    LOGGER.info(
        "Starting JDBC to BigQuery spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5. {},{}"
            + "6. {},{}",
        JDBC_TO_BQ_BIGQUERY_LOCATION,
        bqLocation,
        JDBC_TO_BQ_WAREHOUSE_LOCATION_PROP,
        warehouseLocation,
        JDBC_TO_BQ_INPUT_TABLE_PROP,
        jdbcInputTable,
        JDBC_TO_BQ_INPUT_TABLE_DATABASE_PROP,
        jdbcInputDb,
        JDBC_TO_BQ_APPEND_MODE,
        bqAppendMode,
        JDBC_TO_BQ_JDBC_URL,
        jdbcURL);

    try {
      spark =
          SparkSession.builder()
              .appName("Spark JDBCToBigQuery Job")
              .config(JDBC_TO_BQ_WAREHOUSE_LOCATION_PROP, warehouseLocation)
              .enableHiveSupport()
              .getOrCreate();

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
      Dataset<Row> inputData =
          spark.read().jdbc(jdbcURL, jdbcInputDb + "." + jdbcInputTable, connectionProperties);

      // TODO -- Remove using warehouse location for staging data add new property
      inputData
          .write()
          .mode(bqAppendMode)
          .format("bigquery")
          .option("table", bqLocation)
          .option("temporaryGcsBucket", (warehouseLocation + "/temp/spark").replace("gs://", ""))
          .save();

    } catch (Throwable th) {
      LOGGER.error("Exception in JDBCtoBigquery", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
        throw th;
      }
    }
  }
}
