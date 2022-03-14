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
package com.google.cloud.dataproc.templates.util;

public interface TemplateConstants {

  String DEFAULT_PROPERTY_FILE = "template.properties";

  String PROJECT_ID_PROP = "project.id";
  String BIGTABLE_INSTANCE_ID_PROP = "project.id";
  String BIGTABLE_OUTPUT_TABLE_NAME_PROP = "bigtable.output.table.name";

  String SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID = "spanner.gcs.input.spanner.id";
  String SPANNER_GCS_INPUT_DATABASE_ID = "spanner.gcs.input.database.id";
  String SPANNER_GCS_INPUT_TABLE_ID = "spanner.gcs.input.table.id";
  String SPANNER_GCS_OUTPUT_GCS_PATH = "spanner.gcs.output.gcs.path";
  String SPANNER_GCS_OUTPUT_GCS_SAVEMODE = "spanner.gcs.output.gcs.saveMode";

  /**
   * Column to be used as row key for BigTable. Required for GCSToBigTable template.
   *
   * <p>Note: Key column should be present in input data.
   */
  String BIGTABLE_KEY_COL_PROP = "bigtable.key.col";

  String BIGTABLE_COL_FAMILY_NAME_PROP = "bigtable.col.family.name";

  String GCS_STAGING_BUCKET_PATH = "gcs.staging.bucket.path";

  // HiveToGCS Template configs.

  // Hive warehouse location.
  String HIVE_TO_GCS_OUTPUT_PATH_PROP = "hive.gcs.output.path";
  // Hive warehouse location.
  String HIVE_TO_GCS_OUTPUT_FORMAT_PROP = "hive.gcs.output.format";
  String HIVE_TO_GCS_OUTPUT_FORMAT_DEFAULT = "avro";
  // Hive warehouse location.
  String HIVE_WAREHOUSE_LOCATION_PROP = "spark.sql.warehouse.dir";
  // Hive warehouse location.
  String HIVE_INPUT_TABLE_PROP = "hive.input.table";
  // Hive warehouse location.
  String HIVE_INPUT_TABLE_DATABASE_PROP = "hive.input.db";
  // Optional parameter to pass column name to partition the data while writing it to GCS.
  String HIVE_PARTITION_COL = "hive.partition.col";

  /** Property values for HiveToBQ */
  String HIVE_TO_BQ_BIGQUERY_LOCATION = "hivetobq.bigquery.location";

  String HIVE_TO_BQ_INPUT_TABLE_PROP = "hivetobq.input.table";
  String HIVE_TO_BQ_INPUT_TABLE_DATABASE_PROP = "hivetobq.input.db";
  String HIVE_TO_BQ_APPEND_MODE = "hivetobq.append.mode";
  String HIVE_TO_BQ_PARTITION_COL = "hivetobq.partition.col";
  String HIVE_TO_BQ_WAREHOUSE_LOCATION_PROP = "hivetobq.spark.sql.warehouse.dir";

  /** Property values for JDBCToBQ */
  String JDBC_TO_BQ_BIGQUERY_LOCATION = "jdbctobq.bigquery.location";

  String JDBC_TO_BQ_JDBC_URL = "jdbctobq.jdbc.url";
  String JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME = "jdbctobq.jdbc.driver.class.name";
  String JDBC_TO_BQ_TEMP_GCS_BUCKET = "jdbctobq.temp.gcs.bucket";
  String JDBC_TO_BQ_SQL = "jdbctobq.sql";
  String JDBC_TO_BQ_WRITE_MODE = "jdbctobq.write.mode";

  /** Property values for JDBCToGCS */
  String JDBC_TO_GCS_OUTPUT_LOCATION = "jdbctogcs.output.location";

  String JDBC_TO_GCS_OUTPUT_FORMAT = "jdbctogcs.output.format";
  String JDBC_TO_GCS_JDBC_URL = "jdbctogcs.jdbc.url";
  String JDBC_TO_GCS_JDBC_DRIVER_CLASS_NAME = "jdbctogcs.jdbc.driver.class.name";
  String JDBC_TO_GCS_WRITE_MODE = "jdbctogcs.write.mode";
  String JDBC_TO_GCS_SQL = "jdbctogcs.sql";
  String JDBC_TO_GCS_PARTITION_COLUMN = "jdbctogcs.partition.col";

  /** Property values for WordCount template. */
  String WORD_COUNT_INPUT_PATH_PROP = "word.count.input.path";

  String WORD_COUNT_OUTPUT_PATH_PROP = "word.count.output.path";
  String WORD_COUNT_INPUT_FORMAT_PROP = "word.count.input.format";

  // PubSubToBQ Template configs.

  // Project that contains the input PubSub subscription to be read
  String PUBSUB_INPUT_PROJECT_ID_PROP = "pubsub.input.project.id";
  // PubSub subscription name
  String PUBSUB_INPUT_SUBSCRIPTION_PROP = "pubsub.input.subscription";
  // Stream timeout
  String PUBSUB_TIMEOUT_MS_PROP = "pubsub.timeout.ms";
  // Streaming duration
  String PUBSUB_STREAMING_DURATION_SECONDS_PROP = "pubsub.streaming.duration.seconds";
  // Number of receivers
  String PUBSUB_TOTAL_RECEIVERS_PROP = "pubsub.total.receivers";
  // Project that contains the output table
  String PUBSUB_BQ_OUTPUT_PROJECT_ID_PROP = "pubsub.bq.output.project.id";
  // BigQuery output dataset
  String PUBSUB_BQ_OUTPOUT_DATASET_PROP = "pubsub.bq.output.dataset";
  // BigQuery output table
  String PUBSUB_BQ_OUTPOUT_TABLE_PROP = "pubsub.bq.output.table";
  // Number of records to be written per message to BigQuery
  String PUBSUB_BQ_BATCH_SIZE_PROP = "pubsub.bq.batch.size";

  /** GCS to Bigquery properties */
  String GCS_BQ_INPUT_LOCATION = "gcs.bigquery.input.location";

  String GCS_OUTPUT_DATASET_NAME = "gcs.bigquery.output.dataset";

  String GCS_BQ_INPUT_FORMAT = "gcs.bigquery.input.format";

  String GCS_OUTPUT_TABLE_NAME = "gcs.bigquery.output.table";

  String GCS_BQ_CSV_FORMAT = "csv";

  String GCS_BQ_AVRO_FORMAT = "avro";

  String GCS_BQ_PRQT_FORMAT = "parquet";

  String GCS_BQ_CSV_HEADER = "header";

  String GCS_BQ_OUTPUT_FORMAT = "com.google.cloud.spark.bigquery";

  String GCS_BQ_CSV_INFOR_SCHEMA = "inferSchema";

  String GCS_BQ_TEMP_BUCKET = "temporaryGcsBucket";

  String GCS_BQ_LD_TEMP_BUCKET_NAME = "gcs.bigquery.temp.bucket.name";

  String GCS_BQ_OUTPUT = "table";

  String GCS_BQ_AVRO_EXTD_FORMAT = "com.databricks.spark.avro";

  /** S3 to Bigquery properties */
  String S3_BQ_INPUT_LOCATION = "s3.bq.input.location";

  String S3_BQ_ACCESS_KEY_CONFIG_NAME = "fs.s3a.access.key";

  String S3_BQ_ACCESS_KEY = "s3.bq.access.key";

  String S3_BQ_SECRET_KEY_CONFIG_NAME = "fs.s3a.secret.key";

  String S3_BQ_SECRET_KEY = "s3.bq.secret.key";

  String S3_BQ_ENDPOINT_CONFIG_NAME = "fs.s3a.endpoint";

  String S3_BQ_ENDPOINT_CONFIG_VALUE = "s3.amazonaws.com";

  String S3_BQ_OUTPUT_DATASET_NAME = "s3.bq.output.dataset.name";

  String S3_BQ_OUTPUT_TABLE_NAME = "s3.bq.output.table.name";

  String S3_BQ_LD_TEMP_BUCKET_NAME = "s3.bq.ld.temp.bucket.name";

  String S3_BQ_OUTPUT_FORMAT = "com.google.cloud.spark.bigquery";

  String S3_BQ_HEADER = "header";

  String S3_BQ_OUTPUT = "table";

  String S3_BQ_TEMP_BUCKET = "temporaryGcsBucket";

  String S3_BQ_INPUT_FORMAT = "s3.bq.input.format";

  String S3_BQ_INFER_SCHEMA = "inferSchema";

  String S3_BQ_CSV_FORMAT = "csv";

  String S3_BQ_AVRO_FORMAT = "avro";

  String S3_BQ_PRQT_FORMAT = "parquet";

  String S3_BQ_JSON_FORMAT = "json";

  /** Bigquery to GCS properties */
  String BQ_GCS_INPUT_TABLE_NAME = "bigquery.gcs.input.table";

  String BQ_GCS_OUTPUT_FORMAT_CSV = "csv";

  String BQ_GCS_OUTPUT_FORMAT_AVRO = "avro";

  String BQ_GCS_OUTPUT_FORMAT_PARQUET = "parquet";

  String BQ_GCS_OUTPUT_FORMAT_JSON = "json";

  String BQ_GCS_OUTPUT_FORMAT = "bigquery.gcs.output.format";

  String BQ_GCS_OUTPUT_LOCATION = "bigquery.gcs.output.location";
}
