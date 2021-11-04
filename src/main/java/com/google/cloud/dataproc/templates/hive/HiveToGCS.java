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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_INPUT_TABLE_DATABASE_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_INPUT_TABLE_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_PARTITION_COL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_TO_GCS_OUTPUT_FORMAT_DEFAULT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_TO_GCS_OUTPUT_FORMAT_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_TO_GCS_OUTPUT_PATH_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_WAREHOUSE_LOCATION_PROP;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveToGCS implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(HiveToGCS.class);
  private String outputPath;
  private String warehouseLocation;
  private String hiveInputTable;
  private String hiveInputDb;
  private String outputFormat;
  private String partitionColumn;

  public HiveToGCS() {
    outputPath = getProperties().getProperty(HIVE_TO_GCS_OUTPUT_PATH_PROP);
    warehouseLocation = getProperties().getProperty(HIVE_WAREHOUSE_LOCATION_PROP);
    hiveInputTable = getProperties().getProperty(HIVE_INPUT_TABLE_PROP);
    hiveInputDb = getProperties().getProperty(HIVE_INPUT_TABLE_DATABASE_PROP);
    outputFormat =
        getProperties()
            .getProperty(HIVE_TO_GCS_OUTPUT_FORMAT_PROP, HIVE_TO_GCS_OUTPUT_FORMAT_DEFAULT);
    partitionColumn = getProperties().getProperty(HIVE_PARTITION_COL);
  }

  @Override
  public void runTemplate() {

    if (StringUtils.isAllBlank(outputPath)
        || StringUtils.isAllBlank(hiveInputTable)
        || StringUtils.isAllBlank(hiveInputDb)) {
      LOGGER.error(
          "{},{},{} is required parameter. ",
          HIVE_INPUT_TABLE_PROP,
          HIVE_INPUT_TABLE_DATABASE_PROP,
          HIVE_TO_GCS_OUTPUT_PATH_PROP);
      throw new IllegalArgumentException(
          "Required parameters for HiveToGCS not passed. "
              + "Set mandatory parameter for HiveToGCS template "
              + "in resources/conf/template.properties file.");
    }

    SparkSession spark = null;
    LOGGER.info(
        "Starting Hive to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}",
        HIVE_TO_GCS_OUTPUT_PATH_PROP,
        outputPath,
        HIVE_WAREHOUSE_LOCATION_PROP,
        warehouseLocation,
        HIVE_INPUT_TABLE_PROP,
        hiveInputTable,
        HIVE_INPUT_TABLE_DATABASE_PROP,
        hiveInputDb);

    try {
      spark =
          SparkSession.builder()
              .appName("Spark HiveToGcs Job")
              .config(HIVE_WAREHOUSE_LOCATION_PROP, warehouseLocation)
              .enableHiveSupport()
              .getOrCreate();

      // LOGGER.debug("added jars : {}", spark.sparkContext().addedJars().keys());
      Dataset<Row> inputData = spark.table(hiveInputDb + "." + hiveInputTable);
      List<String> cols = Arrays.asList(inputData.columns());

      LOGGER.info("Columns in table:{} are: {}", hiveInputTable, StringUtils.join(cols, ","));
      LOGGER.info("Total row count: {}", inputData.count());
      LOGGER.info("Writing data to outputPath: {}", outputPath);

      DataFrameWriter<Row> writer = inputData.write().format(outputFormat);

      if (StringUtils.isNotBlank(partitionColumn)) {
        LOGGER.info("Partitioning data by :{} cols", partitionColumn);
        writer.partitionBy(partitionColumn);
      }
      writer.save(outputPath);

      LOGGER.info("HiveToGcs job completed.");
      spark.stop();
    } catch (Throwable th) {
      LOGGER.error("Exception in HiveToGCS", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
