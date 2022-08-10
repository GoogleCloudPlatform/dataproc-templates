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
package com.google.cloud.dataproc.templates.gcs;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.TemplateConstants;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSToBigTable implements BaseTemplate, TemplateConstants {
  private static final Logger LOGGER = LoggerFactory.getLogger(GCSToBigTable.class);

  String inputFileFormat;
  String inputFileLocation;
  String catalog;

  public GCSToBigTable() {
    inputFileLocation = getProperties().getProperty(GCS_TO_BT_INPUT_PATH);
    inputFileFormat = getProperties().getProperty(GCS_TO_BT_INPUT_FILE_FORMAT);
    catalog = getProperties().getProperty(GCS_TO_BT_OUTPUT_CATALOG);
  }

  @Override
  public void runTemplate() throws StreamingQueryException, TimeoutException {

    validateInput();
    SparkSession spark = SparkSession.builder().appName("GCS to BigTable load").getOrCreate();

    Dataset<Row> inputData = spark.read().format(inputFileFormat).load(inputFileLocation);
    inputData
        .write()
        .format("org.apache.hadoop.hbase.spark")
        .option(HBaseTableCatalog.tableCatalog(), catalog.replaceAll("\\s", ""))
        .save();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(inputFileFormat)
        || StringUtils.isAllBlank(inputFileLocation)
        || StringUtils.isAllBlank(catalog)) {
      LOGGER.error(
          "{}, {}, {}, {} is required parameter. ",
          GCS_TO_BT_INPUT_PATH,
          GCS_TO_BT_INPUT_FILE_FORMAT,
          HBASE_TO_GCS_TABLE_CATALOG);
      throw new IllegalArgumentException(
          "Required parameters for GCSToBigTable not passed. "
              + "Set mandatory parameter for GCSToBigTable template "
              + "in resources/conf/template.properties file.");
    }
    LOGGER.info(
        "Starting Hbase to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}",
        GCS_TO_BT_INPUT_PATH,
        inputFileLocation,
        GCS_TO_BT_INPUT_FILE_FORMAT,
        inputFileFormat,
        HBASE_TO_GCS_TABLE_CATALOG,
        catalog);
  }
}
