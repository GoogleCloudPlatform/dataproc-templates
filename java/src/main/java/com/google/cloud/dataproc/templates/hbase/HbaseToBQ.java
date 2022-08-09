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
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseToBQ implements BaseTemplate, TemplateConstants {
  private String catalogue;
  private String bigQueryDataset;
  private String bigQueryTable;
  private String bqTempBucket;

  private static final Logger LOGGER = LoggerFactory.getLogger(HbaseToBQ.class);

  public HbaseToBQ() {
    catalogue = getProperties().getProperty(HBASE_TO_BQ_TABLE_CATALOG);
    bigQueryDataset = getProperties().getProperty(HBASE_TO_BQ_OUTPUT_DATASET_NAME);
    bigQueryTable = getProperties().getProperty(HBASE_TO_BQ_TABLE_NAME);
    bqTempBucket = getProperties().getProperty(HBASE_TO_BQ_TEMP_BUCKET_NAME);
  }

  @Override
  public void runTemplate() throws StreamingQueryException, TimeoutException {
    validateInput();
    SparkSession spark = SparkSession.builder().appName("Spark HbaseToGCS Job").getOrCreate();
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

    dataset
        .write()
        .format("com.google.cloud.spark.bigquery")
        .option("header", true)
        .option("table", bigQueryDataset + "." + bigQueryTable)
        .option("temporaryGcsBucket", bqTempBucket)
        .mode(SaveMode.Append)
        .save();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(bigQueryDataset)
        || StringUtils.isAllBlank(bigQueryTable)
        || StringUtils.isAllBlank(bqTempBucket)
        || StringUtils.isAllBlank(catalogue)) {
      LOGGER.error(
          "{}, {}, {}, {} is required parameter. ",
          HBASE_TO_BQ_TABLE_CATALOG,
          HBASE_TO_BQ_OUTPUT_DATASET_NAME,
          HBASE_TO_BQ_TABLE_NAME,
          HBASE_TO_BQ_TEMP_BUCKET_NAME);
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
        HBASE_TO_BQ_TABLE_CATALOG,
        catalogue,
        HBASE_TO_BQ_OUTPUT_DATASET_NAME,
        bigQueryDataset,
        HBASE_TO_BQ_TABLE_NAME,
        bigQueryTable,
        HBASE_TO_BQ_TEMP_BUCKET_NAME,
        bqTempBucket);
  }
}
