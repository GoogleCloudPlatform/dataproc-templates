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
package com.google.cloud.dataproc.templates.hbase;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.TemplateConstants;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseToGCS implements BaseTemplate, TemplateConstants {

  private static final Logger LOGGER = LoggerFactory.getLogger(HbaseToGCS.class);

  private String catalogue;
  private String outputFileFormat;
  private String gcsSaveMode;
  private String gcsWritePath;
  private final String sparkLogLevel;

  public HbaseToGCS() {
    catalogue = getProperties().getProperty(HBASE_TO_GCS_TABLE_CATALOG);
    outputFileFormat = getProperties().getProperty(HBASE_TO_GCS_OUTPUT_FILE_FORMAT);
    gcsSaveMode = getProperties().getProperty(HBASE_TO_GCS_OUTPUT_SAVE_MODE);
    gcsWritePath = getProperties().getProperty(HBASE_TO_GCS_OUTPUT_PATH);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {
    validateInput();
    SparkSession spark = SparkSession.builder().appName("Spark HbaseToGCS Job").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    Map<String, String> optionsMap = new HashMap<>();
    optionsMap.put(HBaseTableCatalog.tableCatalog(), catalogue.replaceAll("\\s", ""));
    // Read from HBase
    Dataset dataset =
        spark
            .read()
            .format("org.apache.hadoop.hbase.spark")
            .options(optionsMap)
            .option("hbase.spark.use.hbasecontext", "false")
            .load();
    if (dataset != null)
      // Write To GCS
      dataset.write().format(outputFileFormat).mode(gcsSaveMode).save(gcsWritePath);
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(outputFileFormat)
        || StringUtils.isAllBlank(gcsSaveMode)
        || StringUtils.isAllBlank(gcsWritePath)
        || StringUtils.isAllBlank(catalogue)) {
      LOGGER.error(
          "{}, {}, {}, {} is required parameter. ",
          HBASE_TO_GCS_OUTPUT_PATH,
          HBASE_TO_GCS_OUTPUT_FILE_FORMAT,
          HBASE_TO_GCS_OUTPUT_SAVE_MODE,
          HBASE_TO_GCS_TABLE_CATALOG);
      throw new IllegalArgumentException(
          "Required parameters for HbaseToGCS not passed. "
              + "Set mandatory parameter for HbaseToGCS template "
              + "in resources/conf/template.properties file.");
    }
    LOGGER.info(
        "Starting Hbase to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}",
        HBASE_TO_GCS_OUTPUT_PATH,
        gcsWritePath,
        HBASE_TO_GCS_OUTPUT_FILE_FORMAT,
        outputFileFormat,
        HBASE_TO_GCS_OUTPUT_SAVE_MODE,
        gcsSaveMode,
        HBASE_TO_GCS_TABLE_CATALOG,
        catalogue);
  }
}
