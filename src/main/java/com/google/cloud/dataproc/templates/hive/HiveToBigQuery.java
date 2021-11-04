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
 * Usage Instructions -- export PROJECT=yadavaja-sandbox export REGION=us-west1 export
 * JAR=gs://dataproc-templates/jars/dataproc-templates-2.0-SNAPSHOT.jar,gs://dataproc-templates/jars/bigtable-hbase-2.x-hadoop-1.25.0.jar,gs://dataproc-templates/jars/spark-avro_2.12-3.1.0.jar,gs://dataproc-templates/jars/hbase-client-2.0.6.jar,gs://dataproc-templates/jars/shc-core-1.1.1-2.1-s_2.11.jar,gs://dataproc-templates/jars/hbase-common-2.0.6.jar,gs://dataproc-templates/jars/spark-bigquery-with-dependencies_2.12-0.22.2.jar
 *
 * <p>gcloud beta dataproc batches submit spark \ --project=${PROJECT} \ --region=${REGION} \
 * --subnet projects/yadavaja-sandbox/regions/us-west1/subnetworks/test-subnet1 \ --jars=${JAR} \
 * --files=gs://dataproc-templates/conf/core-site.xml,gs://dataproc-templates/conf/hive-site.xml \
 * --properties=spark:spark.hadoop.hive.metastore.uris=thrift://10.218.192.15:9083,spark:spark.hadoop.hive.metastore.warehouse.dir=gs://df-dev-buck/hive/warehouse2,spark:spark.hadoop.javax.jdo.option.ConnectionPassword=hive-password,spark:spark.hadoop.javax.jdo.option.ConnectionDriverName=com.mysql.jdbc.Driver
 * \ --labels job_type=dataproc_template \ --deps-bucket=gs://dataproc-templates \
 * --history-server-cluster=projects/yadavaja-sandbox/regions/us-west1/clusters/per-hs \ --class
 * com.google.cloud.dataproc.templates.main.DataProcTemplate \ -- hivetobigquery
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
      inputData
          .write()
          .mode(bqAppendMode)
          .format("bigquery")
          .option("table", bqLocation)
          .option("temporaryGcsBucket", (warehouseLocation + "/temp/spark").replace("gs://", ""))
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
