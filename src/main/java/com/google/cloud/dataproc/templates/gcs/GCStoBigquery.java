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
package com.google.cloud.dataproc.templates.gcs;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_BQ_INPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_OUTPUT_DATASET_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_OUTPUT_TABLE_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.PROJECT_ID_PROP;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCStoBigquery implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(GCStoBigquery.class);

  private String projectID;
  private String inputFileLocation;
  private String bigQueryDataset;
  private String bigQueryTable;

  public GCStoBigquery() {

    projectID = getProperties().getProperty(PROJECT_ID_PROP);
    inputFileLocation = getProperties().getProperty(GCS_BQ_INPUT_LOCATION);
    bigQueryDataset = getProperties().getProperty(GCS_OUTPUT_DATASET_NAME);
    bigQueryTable = getProperties().getProperty(GCS_OUTPUT_TABLE_NAME);
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(projectID)
        || StringUtils.isAllBlank(inputFileLocation)
        || StringUtils.isAllBlank(bigQueryDataset)
        || StringUtils.isAllBlank(bigQueryTable)) {
      LOGGER.error(
          "{},{},{},{} are required parameter. ",
          PROJECT_ID_PROP,
          GCS_BQ_INPUT_LOCATION,
          GCS_OUTPUT_DATASET_NAME,
          GCS_OUTPUT_TABLE_NAME);
      throw new IllegalArgumentException(
          "Required parameters for GCStoBQ not passed. "
              + "Set mandatory parameter for GCStoBQ template "
              + "in resources/conf/template.properties file.");
    }

    SparkSession spark = null;
    LOGGER.info(
        "Starting GCS to Bigquery spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {},{}",
        GCS_BQ_INPUT_LOCATION,
        inputFileLocation,
        GCS_OUTPUT_DATASET_NAME,
        bigQueryDataset,
        GCS_OUTPUT_TABLE_NAME,
        bigQueryTable);

    try {
      spark = SparkSession.builder().appName("GCS to Bigquery load").getOrCreate();

      Dataset<Row> csvData =
          spark
              .read()
              .format("csv")
              .option("header", true)
              .option("inferSchema", true)
              .load(inputFileLocation);

      csvData
          .write()
          .format("com.google.cloud.spark.bigquery")
          .option("table", bigQueryDataset + "." + bigQueryTable)
          .option("temporaryGcsBucket", "sprak-gcs-to-bq-input")
          .mode(SaveMode.Append)
          .save();
    } catch (Throwable th) {
      LOGGER.error("Exception in HiveToGCS", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
