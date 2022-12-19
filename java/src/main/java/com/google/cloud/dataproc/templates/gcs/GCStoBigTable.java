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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Iterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCStoBigTable implements BaseTemplate, java.io.Serializable {

  public static final Logger LOGGER = LoggerFactory.getLogger(GCStoBigTable.class);

  private String projectID;
  private String inputFileLocation;
  private String bigTableInstanceId;
  private String bigTableTableName;
  private String bigTableProjectId;
  private String bigTableColumnFamily;
  private String inputFileFormat;

  public GCStoBigTable() {

    projectID = getProperties().getProperty(PROJECT_ID_PROP);
    inputFileLocation = getProperties().getProperty(GCS_BT_INPUT_LOCATION);
    bigTableInstanceId = getProperties().getProperty(GCS_BT_OUTPUT_INSTANCE_ID);
    bigTableTableName = getProperties().getProperty(GCS_BT_OUTPUT_TABLE_NAME);
    bigTableProjectId = getProperties().getProperty(GCS_BT_OUTPUT_PROJECT_ID);
    bigTableColumnFamily = getProperties().getProperty(GCS_BT_OUTPUT_TABLE_COLUMN_FAMILY);
    inputFileFormat = getProperties().getProperty(GCS_BT_INPUT_FORMAT);
  }

  @Override
  public void runTemplate() {
    validateInput();

    SparkSession spark = null;
    LOGGER.info("input format: {}", inputFileFormat);

    spark = SparkSession.builder().appName("GCS to Bigtable load").getOrCreate();

    Dataset<Row> inputData = null;

    switch (inputFileFormat) {
      case GCS_BQ_CSV_FORMAT:
        inputData =
            spark
                .read()
                .format(GCS_BQ_CSV_FORMAT)
                .option(GCS_BQ_CSV_HEADER, true)
                .option(GCS_BQ_CSV_INFOR_SCHEMA, true)
                .load(inputFileLocation);
        break;
      case GCS_BQ_AVRO_FORMAT:
        inputData = spark.read().format(GCS_BQ_AVRO_EXTD_FORMAT).load(inputFileLocation);
        break;
      case GCS_BQ_PRQT_FORMAT:
        inputData = spark.read().parquet(inputFileLocation);
        break;
      default:
        throw new IllegalArgumentException(
            "Currently avro, parquet and csv are the only supported formats");
    }

    inputData.foreachPartition(
        new ForeachPartitionFunction<Row>() {
          public void call(Iterator<Row> t) throws Exception {

            BigtableDataClient dataClient =
                BigtableDataClient.create(bigTableProjectId, bigTableInstanceId);

            while (t.hasNext()) {

              long timestamp = System.currentTimeMillis() * 1000;
              Row row = t.next();

              RowMutation rowMutation =
                  RowMutation.create(bigTableTableName, row.get(0).toString());

              for (int i = 0; i < row.size(); i++) {
                rowMutation.setCell(
                    bigTableColumnFamily,
                    row.schema().fieldNames()[i],
                    timestamp,
                    row.get(i).toString());
              }
              dataClient.mutateRow(rowMutation);
            }

            dataClient.close();
          }
        });
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(projectID)
        || StringUtils.isAllBlank(inputFileLocation)
        || StringUtils.isAllBlank(bigTableInstanceId)
        || StringUtils.isAllBlank(bigTableTableName)
        || StringUtils.isAllBlank(inputFileFormat)
        || StringUtils.isAllBlank(bigTableColumnFamily)
        || StringUtils.isAllBlank(bigTableProjectId)) {
      LOGGER.error(
          "{},{},{},{},{},{} are required parameter. ",
          PROJECT_ID_PROP,
          GCS_BT_INPUT_LOCATION,
          GCS_BT_OUTPUT_INSTANCE_ID,
          GCS_BT_OUTPUT_TABLE_NAME,
          GCS_BT_INPUT_FORMAT,
          GCS_BT_OUTPUT_TABLE_COLUMN_FAMILY,
          GCS_BT_OUTPUT_PROJECT_ID);
      throw new IllegalArgumentException(
          "Required parameters for GCStoBT not passed. "
              + "Set mandatory parameter for GCStoBT template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting GCS to BigTable spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {}:{}"
            + "5. {}:{}",
        GCS_BT_INPUT_LOCATION,
        inputFileLocation,
        GCS_BT_OUTPUT_INSTANCE_ID,
        bigTableInstanceId,
        GCS_BT_OUTPUT_TABLE_NAME,
        bigTableTableName,
        GCS_BT_INPUT_FORMAT,
        inputFileFormat,
        GCS_BT_OUTPUT_TABLE_COLUMN_FAMILY,
        bigTableColumnFamily,
        GCS_BT_OUTPUT_PROJECT_ID,
        bigTableProjectId);
  }
}
