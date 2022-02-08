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
package com.google.cloud.dataproc.templates.hive;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark job to move data or/and schema from Hive table to BigQuery. This template can be configured
 * to run in few different modes. In default mode hivetobq.append.mode is set to ErrorIfExists. This
 * will cause failure if target BigQuery table already exists. Other possible values for this
 * property are: 1. Append 2. Overwrite 3. ErrorIfExists 4. Ignore For detailed list of properties
 * refer "HiveToBQ Template properties" section in resources/template.properties file.
 */
public class HiveToBigQuery implements BaseTemplate {
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveToBigQuery.class);
  private String bqLocation;
  private String warehouseLocation;
  private String hiveInputTable;
  private String hiveInputDb;
  private String bqAppendMode;
  private String partitionColumn;

  public HiveToBigQuery() {
    bqLocation = getProperties().getProperty(HIVE_TO_BQ_BIGQUERY_LOCATION);
    warehouseLocation = getProperties().getProperty(HIVE_TO_BQ_WAREHOUSE_LOCATION_PROP);
    hiveInputTable = getProperties().getProperty(HIVE_TO_BQ_INPUT_TABLE_PROP);
    hiveInputDb = getProperties().getProperty(HIVE_TO_BQ_INPUT_TABLE_DATABASE_PROP);
    bqAppendMode = getProperties().getProperty(HIVE_TO_BQ_APPEND_MODE);
    partitionColumn = getProperties().getProperty(HIVE_TO_BQ_PARTITION_COL);
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(bqLocation)
        || StringUtils.isAllBlank(hiveInputTable)
        || StringUtils.isAllBlank(warehouseLocation)
        || StringUtils.isAllBlank(hiveInputDb)) {
      LOGGER.error(
          "{},{},{},{} is required parameter. ",
          HIVE_TO_BQ_BIGQUERY_LOCATION,
          HIVE_TO_BQ_INPUT_TABLE_PROP,
          HIVE_TO_BQ_INPUT_TABLE_DATABASE_PROP,
          HIVE_TO_BQ_WAREHOUSE_LOCATION_PROP);
      throw new IllegalArgumentException(
          "Required parameters for HiveToBigQuery not passed. "
              + "Set mandatory parameter for HiveToBigQuery template "
              + "in resources/conf/template.properties file.");
    }

    SparkSession spark = null;
    LOGGER.info(
        "Starting Hive to BigQuery spark jo;b with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5. {},{}",
        HIVE_TO_BQ_BIGQUERY_LOCATION,
        bqLocation,
        HIVE_TO_BQ_WAREHOUSE_LOCATION_PROP,
        warehouseLocation,
        HIVE_TO_BQ_INPUT_TABLE_PROP,
        hiveInputTable,
        HIVE_TO_BQ_INPUT_TABLE_DATABASE_PROP,
        hiveInputDb,
        HIVE_TO_BQ_APPEND_MODE,
        bqAppendMode);
    try {
      // Initialize Spark session
      spark =
          SparkSession.builder()
              .appName("Spark HiveToBigQuery Job")
              .config(HIVE_TO_BQ_WAREHOUSE_LOCATION_PROP, warehouseLocation)
              .enableHiveSupport()
              .getOrCreate();

      LOGGER.debug("added jars : {}", spark.sparkContext().addedJars().keys());

      /** Read Input data from Hive table */
      Dataset<Row> inputData = spark.sql("select * from " + hiveInputDb + "." + hiveInputTable);

      /**
       * Write output to BigQuery
       *
       * <p>Warehouse location to be used as temporary GCS bucket location for staging data before
       * writing to BQ. Job failures would require a manual cleanup of this data.
       */
      // TODO -- Remove using warehouse location for staging data add new property

      /*inputData
         .write()
         .mode(bqAppendMode)
         .format("bigquery")
         .option("table", bqLocation)
         .option("temporaryGcsBucket", (warehouseLocation + "/temp/spark").replace("gs://", ""))
         .save();


      */

      inputData
          .write()
          .format(GCS_BQ_OUTPUT_FORMAT)
          .option(GCS_BQ_OUTPUT, bqLocation)
          .option(GCS_BQ_TEMP_BUCKET, warehouseLocation)
          .mode(bqAppendMode)
          .save();

      LOGGER.info("HiveToBigQuery job completed.");
      spark.stop();
    } catch (Throwable th) {
      LOGGER.error("Exception in HiveToBigQuery", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
