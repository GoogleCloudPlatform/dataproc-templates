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

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCStoBigTable implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(GCStoBigTable.class);
  private final GCStoBigTableConfig gcStoBigTableConfig;

  public GCStoBigTable(GCStoBigTableConfig gcStoBigTableConfig) {
    this.gcStoBigTableConfig = gcStoBigTableConfig;
  }

  public static GCStoBigTable of(String... args) {
    GCStoBigTableConfig gcStoBigTableConfig =
        GCStoBigTableConfig.fromProperties(PropertyUtil.getProperties());
    LOGGER.info("Config loaded\n{}", gcStoBigTableConfig);
    return new GCStoBigTable(gcStoBigTableConfig);
  }

  @Override
  public void validateInput() throws IllegalArgumentException {
    ValidationUtil.validateOrThrow(gcStoBigTableConfig);
  }

  private String getBigTableCatalog(String projectID, String gcsPath) {
    LOGGER.info("Create Storage Object");
    Storage storage = StorageOptions.newBuilder().setProjectId(projectID).build().getService();
    Blob blob = storage.get(BlobId.fromGsUtilUri(gcsPath));

    return new String(blob.getContent());
  }

  @Override
  public void runTemplate()
      throws StreamingQueryException, TimeoutException, SQLException, InterruptedException {

    LOGGER.info("Initialize the Spark session");
    SparkSession spark = SparkSession.builder().appName("Spark GCStoBigTable Job").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(gcStoBigTableConfig.getSparkLogLevel());

    LOGGER.info("Read Data");
    Dataset<Row> inputData;
    switch (gcStoBigTableConfig.getInputFileFormat()) {
      case GCS_BQ_CSV_FORMAT:
        inputData =
            spark
                .read()
                .format(GCS_BQ_CSV_FORMAT)
                .option(GCS_BQ_CSV_HEADER, true)
                .option(GCS_BQ_CSV_INFOR_SCHEMA, true)
                .load(gcStoBigTableConfig.getInputFileLocation());
        break;
      case GCS_BQ_AVRO_FORMAT:
        inputData =
            spark
                .read()
                .format(GCS_BQ_AVRO_EXTD_FORMAT)
                .load(gcStoBigTableConfig.getInputFileLocation());
        break;
      case GCS_BQ_PRQT_FORMAT:
        inputData = spark.read().parquet(gcStoBigTableConfig.getInputFileLocation());
        break;
      default:
        throw new IllegalArgumentException(
            "Currently avro, parquet and csv are the only supported formats");
    }

    LOGGER.info("Retrieve Catalog");
    String catalog =
        getBigTableCatalog(
            gcStoBigTableConfig.getProjectId(), gcStoBigTableConfig.getBigTableCatalogLocation());

    LOGGER.info("Input File Schema:  \n{}", inputData.schema().prettyJson());
    LOGGER.info("BigTable Catalog:  \n{}", catalog);

    LOGGER.info("Write To BigTable");
    inputData
        .write()
        .format(SPARK_BIGTABLE_FORMAT)
        .option(SPARK_BIGTABLE_CATALOG, catalog)
        .option(SPARK_BIGTABLE_PROJECT_ID, gcStoBigTableConfig.getBigTableProjectId())
        .option(SPARK_BIGTABLE_INSTANCE_ID, gcStoBigTableConfig.getBigTableInstanceId())
        .option(SPARK_BIGTABLE_CREATE_NEW_TABLE, gcStoBigTableConfig.getIsCreateBigTable())
        .option(SPARK_BIGTABLE_BATCH_MUTATE_SIZE, gcStoBigTableConfig.getBigTableBatchMutateSize())
        .save();
  }
}
