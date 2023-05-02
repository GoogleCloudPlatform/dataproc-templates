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
import java.util.Arrays;
import java.util.List;
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
  private String hiveInputTable;
  private String hiveInputDb;
  private String outputFormat;
  private String partitionColumn;
  private String gcsSaveMode;
  private String tempTable;
  private String tempQuery;
  private final String sparkLogLevel;

  /**
   * Spark job to move data from Hive table to GCS bucket. For detailed list of properties refer
   * "HiveToGCS Template properties." section in resources/template.properties file.
   *
   * <p>Usage Instructions bin/start.sh gs://dataproc-templates/jars \ gcp-project \ gcp-region \
   * network-subnet \ persistent-history-server[Optional] \ hivetogcs
   */
  public HiveToGCS() {
    outputPath = getProperties().getProperty(HIVE_TO_GCS_OUTPUT_PATH_PROP);
    hiveInputTable = getProperties().getProperty(HIVE_INPUT_TABLE_PROP);
    hiveInputDb = getProperties().getProperty(HIVE_INPUT_TABLE_DATABASE_PROP);
    outputFormat =
        getProperties()
            .getProperty(HIVE_TO_GCS_OUTPUT_FORMAT_PROP, HIVE_TO_GCS_OUTPUT_FORMAT_DEFAULT);
    partitionColumn = getProperties().getProperty(HIVE_PARTITION_COL);
    gcsSaveMode = getProperties().getProperty(HIVE_GCS_SAVE_MODE);
    tempTable = getProperties().getProperty(HIVE_GCS_TEMP_TABLE);
    tempQuery = getProperties().getProperty(HIVE_GCS_TEMP_QUERY);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {

    // Confiure spark session to read from hive.
    SparkSession spark =
        SparkSession.builder().appName("Spark HiveToGcs Job").enableHiveSupport().getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    // Read source Hive table.
    Dataset<Row> inputData = spark.table(hiveInputDb + "." + hiveInputTable);
    List<String> cols = Arrays.asList(inputData.columns());

    LOGGER.info("Columns in table:{} are: {}", hiveInputTable, StringUtils.join(cols, ","));
    LOGGER.info("Total row count: {}", inputData.count());
    LOGGER.info("Writing data to outputPath: {}", outputPath);

    if (StringUtils.isNotBlank(tempTable) && StringUtils.isNotBlank(tempQuery)) {
      inputData.createOrReplaceGlobalTempView(tempTable);
      inputData = spark.sql(tempQuery);
    }

    DataFrameWriter<Row> writer = inputData.write().format(outputFormat);

    /*
     * If optional partition column is passed than partition data by partition
     * column before writing to GCS.
     * */
    if (StringUtils.isNotBlank(partitionColumn)) {
      LOGGER.info("Partitioning data by :{} cols", partitionColumn);
      writer.partitionBy(partitionColumn);
    }
    writer.mode(gcsSaveMode).save(outputPath);

    LOGGER.info("HiveToGcs job completed.");
    spark.stop();
  }

  public void validateInput() {
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

    LOGGER.info(
        "Starting Hive to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}",
        HIVE_TO_GCS_OUTPUT_PATH_PROP,
        outputPath,
        HIVE_INPUT_TABLE_PROP,
        hiveInputTable,
        HIVE_INPUT_TABLE_DATABASE_PROP,
        hiveInputDb);
  }
}
