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
  String SPARK_LOG_LEVEL = "log.level";
  String BIGTABLE_INSTANCE_ID_PROP = "project.id";
  String BIGTABLE_OUTPUT_TABLE_NAME_PROP = "bigtable.output.table.name";
  String SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID = "spanner.gcs.input.spanner.id";
  String SPANNER_GCS_INPUT_DATABASE_ID = "spanner.gcs.input.database.id";
  String SPANNER_GCS_INPUT_TABLE_ID = "spanner.gcs.input.table.id";
  String SPANNER_GCS_OUTPUT_GCS_PATH = "spanner.gcs.output.gcs.path";
  String SPANNER_GCS_OUTPUT_GCS_SAVEMODE = "spanner.gcs.output.gcs.saveMode";
  String SPANNER_GCS_OUTPUT_FORMAT = "spanner.gcs.output.gcs.format";
  String SPANNER_GCS_INPUT_SQL_PARTITION_COLUMN = "spanner.gcs.input.sql.partitionColumn";
  String SPANNER_GCS_INPUT_SQL_LOWER_BOUND = "spanner.gcs.input.sql.lowerBound";
  String SPANNER_GCS_INPUT_SQL_UPPER_BOUND = "spanner.gcs.input.sql.upperBound";
  String SPANNER_GCS_INPUT_SQL_NUM_PARTITIONS = "spanner.gcs.input.sql.numPartitions";
  String SPANNER_GCS_TEMP_TABLE = "spanner.gcs.temp.table";
  String SPANNER_GCS_TEMP_QUERY = "spanner.gcs.temp.query";
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
  String HIVE_GCS_TEMP_TABLE = "hive.gcs.temp.table";
  String HIVE_GCS_TEMP_QUERY = "hive.gcs.temp.query";
  // Hive warehouse location.
  String HIVE_WAREHOUSE_LOCATION_PROP = "spark.sql.warehouse.dir";
  // Hive warehouse location.
  String HIVE_INPUT_TABLE_PROP = "hive.input.table";
  // Hive warehouse location.
  String HIVE_INPUT_TABLE_DATABASE_PROP = "hive.input.db";
  // Optional parameter to pass column name to partition the data while writing it to GCS.
  String HIVE_PARTITION_COL = "hive.partition.col";
  String HIVE_GCS_SAVE_MODE = "hive.gcs.save.mode";

  /** Property values for HiveToBQ */
  String HIVE_TO_BQ_BIGQUERY_LOCATION = "hivetobq.bigquery.location";

  String HIVE_TO_BQ_SQL = "hivetobq.sql";
  String HIVE_TO_BQ_APPEND_MODE = "hivetobq.write.mode";
  String HIVE_TO_BQ_TEMP_GCS_BUCKET = "hivetobq.temp.gcs.bucket";
  String HIVE_TO_BQ_TEMP_TABLE = "hivetobq.temp.table";
  String HIVE_TO_BQ_TEMP_QUERY = "hivetobq.temp.query";

  /** Property values for HbaseToGCS */
  String HBASE_TO_GCS_OUTPUT_FILE_FORMAT = "hbasetogcs.output.fileformat";

  String HBASE_TO_GCS_OUTPUT_SAVE_MODE = "hbasetogcs.output.savemode";
  String HBASE_TO_GCS_OUTPUT_PATH = "hbasetogcs.output.path";
  String HBASE_TO_GCS_TABLE_CATALOG = "hbasetogcs.table.catalog";

  /** Property values for CassandraToGCS */
  String CASSANDRA_TO_GSC_INPUT_KEYSPACE = "cassandratogcs.input.keyspace";

  String CASSANDRA_TO_GSC_INPUT_TABLE = "cassandratogcs.input.table";
  String CASSANDRA_TO_GSC_INPUT_HOST = "cassandratogcs.input.host";
  String CASSANDRA_TO_GSC_OUTPUT_FORMAT = "cassandratogcs.output.format";
  String CASSANDRA_TO_GSC_OUTPUT_SAVE_MODE = "cassandratogcs.output.savemode";
  String CASSANDRA_TO_GSC_OUTPUT_PATH = "cassandratogcs.output.path";
  String CASSANDRA_TO_GSC_INPUT_CATALOG = "cassandratogcs.input.catalog.name";
  String CASSANDRA_TO_GSC_INPUT_QUERY = "cassandratogcs.input.query";

  /** Property values for JDBCToBQ */
  String JDBC_TO_BQ_BIGQUERY_LOCATION = "jdbctobq.bigquery.location";

  String JDBC_TO_BQ_JDBC_URL = "jdbctobq.jdbc.url";
  String JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME = "jdbctobq.jdbc.driver.class.name";
  String JDBC_TO_BQ_JDBC_FETCH_SIZE = "jdbctobq.jdbc.fetchsize";
  String JDBC_TO_BQ_JDBC_SESSION_INIT_STATEMENT = "jdbctobq.jdbc.sessioninitstatement";
  String JDBC_TO_BQ_TEMP_GCS_BUCKET = "jdbctobq.temp.gcs.bucket";
  String JDBC_TO_BQ_SQL = "jdbctobq.sql";
  String JDBC_TO_BQ_SQL_PARTITION_COLUMN = "jdbctobq.sql.partitionColumn";
  String JDBC_TO_BQ_SQL_LOWER_BOUND = "jdbctobq.sql.lowerBound";
  String JDBC_TO_BQ_SQL_UPPER_BOUND = "jdbctobq.sql.upperBound";
  String JDBC_TO_BQ_SQL_NUM_PARTITIONS = "jdbctobq.sql.numPartitions";
  String JDBC_TO_BQ_WRITE_MODE = "jdbctobq.write.mode";
  String JDBC_BQ_TEMP_TABLE = "jdbc.bq.temp.table";
  String JDBC_BQ_TEMP_QUERY = "jdbc.bq.temp.query";

  /** Property values for JDBCToGCS */
  String JDBC_TO_GCS_OUTPUT_LOCATION = "jdbctogcs.output.location";

  String JDBC_TO_GCS_OUTPUT_FORMAT = "jdbctogcs.output.format";
  String JDBC_TO_GCS_JDBC_URL = "jdbctogcs.jdbc.url";
  String JDBC_TO_GCS_JDBC_DRIVER_CLASS_NAME = "jdbctogcs.jdbc.driver.class.name";
  String JDBC_TO_GCS_JDBC_FETCH_SIZE = "jdbctogcs.jdbc.fetchsize";
  String JDBC_TO_GCS_JDBC_SESSION_INIT_STATEMENT = "jdbctogcs.jdbc.sessioninitstatement";
  String JDBC_TO_GCS_WRITE_MODE = "jdbctogcs.write.mode";
  String JDBC_TO_GCS_SQL = "jdbctogcs.sql";
  String JDBC_TO_GCS_SQL_FILE = "jdbctogcs.sql.file";
  String JDBC_TO_GCS_SQL_PARTITION_COLUMN = "jdbctogcs.sql.partitionColumn";
  String JDBC_TO_GCS_SQL_LOWER_BOUND = "jdbctogcs.sql.lowerBound";
  String JDBC_TO_GCS_SQL_UPPER_BOUND = "jdbctogcs.sql.upperBound";
  String JDBC_TO_GCS_SQL_NUM_PARTITIONS = "jdbctogcs.sql.numPartitions";
  String JDBC_TO_GCS_OUTPUT_PARTITION_COLUMN = "jdbctogcs.output.partition.col";
  String JDBC_TO_GCS_TEMP_TABLE = "jdbctogcs.temp.table";
  String JDBC_TO_GCS_TEMP_QUERY = "jdbctogcs.temp.query";

  /** Property values for JDBCToSpanner */
  String JDBC_TO_SPANNER_JDBC_URL = "jdbctospanner.jdbc.url";

  String JDBC_TO_SPANNER_JDBC_DRIVER_CLASS_NAME = "jdbctospanner.jdbc.driver.class.name";
  String JDBC_TO_SPANNER_JDBC_FETCH_SIZE = "jdbctospanner.jdbc.fetchsize";
  String JDBC_TO_SPANNER_JDBC_SESSION_INIT_STATEMENT = "jdbctospanner.jdbc.sessioninitstatement";
  String JDBC_TO_SPANNER_SQL = "jdbctospanner.sql";
  String JDBC_TO_SPANNER_SQL_FILE = "jdbctospanner.sql.file";
  String JDBC_TO_SPANNER_SQL_PARTITION_COLUMN = "jdbctospanner.sql.partitionColumn";
  String JDBC_TO_SPANNER_SQL_LOWER_BOUND = "jdbctospanner.sql.lowerBound";
  String JDBC_TO_SPANNER_SQL_UPPER_BOUND = "jdbctospanner.sql.upperBound";
  String JDBC_TO_SPANNER_SQL_NUM_PARTITIONS = "jdbctospanner.sql.numPartitions";
  String JDBC_TO_SPANNER_TEMP_TABLE = "jdbctospanner.temp.table";
  String JDBC_TO_SPANNER_TEMP_QUERY = "jdbctospanner.temp.query";
  String JDBC_TO_SPANNER_OUTPUT_INSTANCE = "jdbctospanner.output.instance";
  String JDBC_TO_SPANNER_OUTPUT_DATABASE = "jdbctospanner.output.database";
  String JDBC_TO_SPANNER_OUTPUT_TABLE = "jdbctospanner.output.table";
  String JDBC_TO_SPANNER_OUTPUT_SAVE_MODE = "jdbctospanner.output.saveMode";
  String JDBC_TO_SPANNER_OUTPUT_PRIMARY_KEY = "jdbctospanner.output.primaryKey";
  String JDBC_TO_SPANNER_OUTPUT_BATCH_INSERT_SIZE = "jdbctospanner.output.batch.size";

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

  /** Property values for PubSub To BigTable */
  String PUBSUB_BIGTABLE_OUTPUT_INSTANCE_ID_PROP = "pubsub.bigtable.output.instance.id";
  // BigTable Instance Id
  String PUBSUB_BIGTABLE_OUTPUT_PROJECT_ID_PROP = "pubsub.bigtable.output.project.id";
  // Project that contains the output table
  String PUBSUB_BIGTABLE_OUTPUT_TABLE_PROP = "pubsub.bigtable.output.table";
  // BigTable table name
  String ROWKEY = "rowkey";
  String COLUMN_FAMILY = "columnfamily";
  String COLUMN_NAME = "columnname";
  String COLUMN_VALUE = "columnvalue";
  String COLUMNS = "columns";

  /** Property values for PubSubLite To BigTable */
  // Many constants are reused from Pubsub to Bigtable
  String PUBSUBLITE_INPUT_PROJECT_ID_PROP = "pubsublite.input.project.id";
  // PubSub subscription name
  String PUBSUBLITE_INPUT_SUBSCRIPTION_PROP = "pubsublite.input.subscription";
  // Stream timeout
  String PUBSUBLITE_TIMEOUT_MS_PROP = "pubsublite.timeout.ms";
  // Streaming duration
  String PUBSUBLITE_STREAMING_DURATION_SECONDS_PROP = "pubsublite.streaming.duration.seconds";
  // Checkpoint location
  String PUBSUBLITE_CHECKPOINT_LOCATION_PROP = "pubsublite.checkpoint.location";
  // BigTable Instance Id
  String PUBSUBLITE_BIGTABLE_OUTPUT_INSTANCE_ID_PROP = "pubsublite.bigtable.output.instance.id";
  // Project that contains the output table
  String PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT_ID_PROP = "pubsublite.bigtable.output.project.id";
  // BigTable table name
  String PUBSUBLITE_BIGTABLE_OUTPUT_TABLE_PROP = "pubsublite.bigtable.output.table";
  String PUBSUBLITE_FORMAT = "pubsublite";
  String PUBSUBLITE_CHECKPOINT = "checkpointLocation";
  String PUBSUBLITE_SUBSCRIPTION = "pubsublite.subscription";
  // Constants to be used for parsing the JSON from pubsub

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
  String GCS_BQ_CSV_DELIMITER_PROP_NAME = "delimiter";
  String GCS_BQ_TEMP_BUCKET = "temporaryGcsBucket";
  String GCS_BQ_LD_TEMP_BUCKET_NAME = "gcs.bigquery.temp.bucket.name";
  String GCS_BQ_TEMP_TABLE = "gcs.bigquery.temp.table";
  String GCS_BQ_TEMP_QUERY = "gcs.bigquery.temp.query";
  String GCS_BQ_OUTPUT = "table";
  String GCS_BQ_AVRO_EXTD_FORMAT = "com.databricks.spark.avro";
  String GCS_BQ_OUTPUT_MODE = "gcs.bigquery.output.mode";

  /** GCS to BigTable properties */
  String GCS_BT_INPUT_LOCATION = "gcs.bigtable.input.location";

  String GCS_BT_INPUT_FORMAT = "gcs.bigtable.input.format";
  String GCS_BT_OUTPUT_INSTANCE_ID = "gcs.bigtable.output.instance.id";
  String GCS_BT_OUTPUT_PROJECT_ID = "gcs.bigtable.output.project.id";
  String GCS_BT_OUTPUT_TABLE_NAME = "gcs.bigtable.table.name";
  String GCS_BT_OUTPUT_TABLE_COLUMN_FAMILY = "gcs.bigtable.column.family";

  /** GCS to GCS properties */
  String GCS_GCS_INPUT_LOCATION = "gcs.gcs.input.location";

  String GCS_GCS_INPUT_FORMAT = "gcs.gcs.input.format";
  String GCS_GCS_OUTPUT_LOCATION = "gcs.gcs.output.location";
  String GCS_GCS_OUTPUT_FORMAT = "gcs.gcs.output.format";
  String GCS_GCS_WRITE_MODE = "gcs.gcs.write.mode";
  String GCS_GCS_OUTPUT_PARTITION_COLUMN = "gcs.gcs.output.partition.col";
  String GCS_GCS_TEMP_TABLE = "gcs.gcs.temp.table";
  String GCS_GCS_TEMP_QUERY = "gcs.gcs.temp.query";

  /** GCS to JDBC properties */
  String GCS_JDBC_AVRO_FORMAT = "avro";

  String GCS_JDBC_CSV_FORMAT = "csv";
  String GCS_JDBC_CSV_HEADER = "header";
  String GCS_JDBC_CSV_INFER_SCHEMA = "inferSchema";
  String GCS_JDBC_ORC_FORMAT = "orc";
  String GCS_JDBC_PRQT_FORMAT = "parquet";

  /** MongoDb common properties */
  String MONGO_FORMAT = "com.mongodb.spark.sql.DefaultSource";

  String MONGO_URL = "spark.mongodb.output.uri";
  String MONGO_INPUT_URI = "spark.mongodb.input.uri";
  String MONGO_DATABASE = "database";
  String MONGO_COLLECTION = "collection";
  String MONGO_BATCH_SIZE = "maxBatchSize";
  String MONGO_DEFAULT_BATCH_SIZE = "512";

  /** GCS to Mongo properties */
  String GCS_MONGO_INPUT_FORMAT = "gcs.mongodb.input.format";

  String GCS_MONGO_INPUT_LOCATION = "gcs.mongodb.input.location";
  String GCS_MONGO_URL = "gcs.mongodb.output.uri";
  String GCS_MONGO_DATABASE = "gcs.mongodb.output.database";
  String GCS_MONGO_COLLECTION = "gcs.mongodb.output.collection";
  String GCS_MONGO_BATCH_SIZE = "gcs.mongodb.batch.size";
  String GCS_MONGO_OUTPUT_MODE = "gcs.mongodb.output.mode";

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
  String S3_BQ_OUTPUT_MODE = "s3.bq.output.mode";

  /** Cassandra to BQ properties */
  String CASSANDRA_TO_BQ_INPUT_KEYSPACE = "cassandratobq.input.keyspace";

  String CASSANDRA_TO_BQ_INPUT_TABLE = "cassandratobq.input.table";
  String CASSANDRA_TO_BQ_INPUT_HOST = "cassandratobq.input.host";
  String CASSANDRA_TO_BQ_BIGQUERY_LOCATION = "cassandratobq.bigquery.location";
  String CASSANDRA_TO_BQ_WRITE_MODE = "cassandratobq.output.mode";
  String CASSANDRA_TO_BQ_TEMP_LOCATION = "cassandratobq.temp.gcs.location";
  String CASSANDRA_TO_BQ_QUERY = "cassandratobq.input.query";
  String CASSANDRA_TO_BQ_CATALOG = "cassandratobq.input.catalog.name";

  /** Bigquery to GCS properties */
  String BQ_GCS_INPUT_TABLE_NAME = "bigquery.gcs.input.table";

  String BQ_GCS_OUTPUT_FORMAT_CSV = "csv";
  String BQ_GCS_OUTPUT_FORMAT_AVRO = "avro";
  String BQ_GCS_OUTPUT_FORMAT_PARQUET = "parquet";
  String BQ_GCS_OUTPUT_FORMAT_JSON = "json";
  String BQ_GCS_OUTPUT_FORMAT = "bigquery.gcs.output.format";
  String BQ_GCS_OUTPUT_LOCATION = "bigquery.gcs.output.location";
  String BQ_GCS_OUTPUT_MODE = "bigquery.gcs.output.mode";

  /** RedShift to GCS properties */
  String REDSHIFT_AWS_INPUT_URL = "redshift.aws.input.url";

  String REDSHIFT_AWS_INPUT_TABLE = "redshift.aws.input.table";
  String REDSHIFT_AWS_TEMP_DIR = "redshift.aws.input.temp.dir";
  String REDSHIFT_AWS_INPUT_IAM_ROLE = "redshift.aws.input.iam.role";
  String REDSHIFT_AWS_INPUT_ACCESS_KEY = "redshift.aws.input.access.key";
  String REDSHIFT_AWS_INPUT_SECRET_KEY = "redshift.aws.input.secret.key";
  String REDSHIFT_GCS_OUTPUT_FILE_FORMAT = "redshift.gcs.output.file.format";
  String REDSHIFT_GCS_OUTPUT_FILE_LOCATION = "redshift.gcs.output.file.location";
  String REDSHIFT_GCS_OUTPUT_MODE = "redshift.gcs.output.mode";
  String REDSHIFT_GCS_TEMP_TABLE = "redshift.gcs.temp.table";
  String REDSHIFT_GCS_TEMP_QUERY = "redshift.gcs.temp.query";

  /** PubSubToGCS Template configs. */
  // Project that contains the input PubSub subscription to be read
  String PUBSUB_GCS_INPUT_PROJECT_ID_PROP = "pubsubtogcs.input.project.id";
  // PubSub subscription name
  String PUBSUB_GCS_INPUT_SUBSCRIPTION_PROP = "pubsubtogcs.input.subscription";
  // Stream timeout
  String PUBSUB_GCS_TIMEOUT_MS_PROP = "pubsubtogcs.timeout.ms";
  // Streaming duration
  String PUBSUB_GCS_STREAMING_DURATION_SECONDS_PROP = "pubsubtogcs.streaming.duration.seconds";
  // Number of receivers
  String PUBSUB_GCS_TOTAL_RECEIVERS_PROP = "pubsubtogcs.total.receivers";
  // GCS bucket URL
  String PUBSUB_GCS_BUCKET_NAME = "pubsubtogcs.gcs.bucket.name";
  // Number of records to be written per message to GCS
  String PUBSUB_GCS_BATCH_SIZE_PROP = "pubsubtogcs.batch.size";
  String PUBSUB_GCS_BUCKET_OUTPUT_PATH = "output/";
  String PUBSUB_GCS_OUTPUT_DATA_FORMAT = "pubsubtogcs.gcs.output.data.format";
  String PUBSUB_GCS_AVRO_EXTENSION = "avro";
  String PUBSUB_GCS_JSON_EXTENSION = "json";

  /** Dataplex GCS to BQ */
  String DATAPLEX_GCS_BQ_TARGET_DATASET = "dataplex.gcs.bq.target.dataset";

  String DATAPLEX_GCS_BQ_TARGET_ASSET = "dataplex.gcs.bq.target.asset";
  String DATAPLEX_GCS_BQ_TARGET_ENTITY = "dataplex.gcs.bq.target.entity";
  String DATAPLEX_GCS_BQ_SAVE_MODE = "dataplex.gcs.bq.save.mode";
  String DATAPLEX_GCS_BQ_INCREMENTAL_PARTITION_COPY = "dataplex.gcs.bq.incremental.partition.copy";
  String DATAPLEX_GCS_BQ_BASE_PATH_PROP_NAME = "basePath";
  String DATAPLEX_GCS_BQ_CREATE_DISPOSITION_PROP_NAME = "createDisposition";
  String DATAPLEX_GCS_BQ_CREATE_DISPOSITION_CREATE_IF_NEEDED = "CREATE_IF_NEEDED";
  String DATAPLEX_GCS_BQ_PARTITION_FIELD_PROP_NAME = "partitionField";
  String DATAPLEX_GCS_BQ_PARTITION_TYPE_PROP_NAME = "partitionType";
  String SPARK_CONF_NAME_VIEWS_ENABLED = "viewsEnabled";
  String SPARK_CONF_NAME_MATERIALIZATION_PROJECT = "materializationProject";
  String SPARK_CONF_NAME_MATERIALIZATION_DATASET = "materializationDataset";
  String SPARK_READ_FORMAT_BIGQUERY = "com.google.cloud.spark.bigquery";
  String INTERMEDIATE_FORMAT_OPTION_NAME = "intermediateFormat";
  String INTERMEDIATE_FORMAT_ORC = "orc";

  /** Kafka To PubSub properties */
  String KAFKA_PUBSUB_CHECKPOINT_LOCATION = "kafka.pubsub.checkpoint.location";

  String KAFKA_PUBSUB_BOOTSTRAP_SERVERS = "kafka.pubsub.bootstrap.servers";
  String KAFKA_PUBSUB_INPUT_TOPIC = "kafka.pubsub.input.topic";
  String KAFKA_PUBSUB_OUTPUT_PROJECT_ID = "kafka.pubsub.output.projectId";
  String KAFKA_PUBSUB_OUTPUT_TOPIC = "kafka.pubsub.output.topic";
  String KAFKA_PUBSUB_STARTING_OFFSET = "kafka.pubsub.starting.offset";
  String KAFKA_PUBSUB_AWAIT_TERMINATION_TIMEOUT = "kafka.pubsub.await.termination.timeout";

  String SPARK_SAVE_MODE_OVERWRITE = "overwrite";
  String SPARK_SAVE_MODE_APPEND = "append";
  String SPARK_SAVE_MODE_IGNORE = "ignore";
  String SPARK_SAVE_MODE_ERRORIFEXISTS = "errorifexists";
  String SPARK_FILE_FORMAT_CSV = "csv";
  String SPARK_CSV_HEADER = "header";
  String SPARK_FILE_FORMAT_PARQUET = "parquet";
  String SPARK_FILE_FORMAT_AVRO = "avro";
  String SPARK_FILE_FORMAT_JSON = "json";
  String SPARK_INFER_SCHEMA = "inferSchema";
  String BQ_TABLE_NAME_FORMAT = "%s.%s.%s";

  /** KafkaToBQ properties */
  String KAFKA_BQ_CHECKPOINT_LOCATION = "kafka.bq.checkpoint.location";

  String KAFKA_BQ_SPARK_CONF_NAME_INPUT_FORMAT = "kafka";
  String KAFKA_BQ_SPARK_CONF_NAME_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  String KAFKA_BQ_SPARK_CONF_NAME_SUBSCRIBE = "subscribe";
  String KAFKA_BQ_SPARK_CONF_NAME_STARTING_OFFSETS = "startingOffsets";
  String KAFKA_BQ_SPARK_CONF_NAME_FAIL_ON_DATA_LOSS = "failOnDataLoss";
  String KAFKA_BQ_SPARK_CONF_NAME_OUTPUT_FORMAT = "com.google.cloud.spark.bigquery";
  String KAFKA_BQ_SPARK_CONF_NAME_OUTPUT_HEADER = "header";
  String KAFKA_BQ_SPARK_CONF_NAME_CHECKPOINT_LOCATION = "checkpointLocation";
  String KAFKA_BQ_SPARK_CONF_NAME_TABLE = "table";
  String KAFKA_BQ_SPARK_CONF_NAME_TEMP_GCS_BUCKET = "temporaryGcsBucket";
  String KAFKA_BQ_BOOTSTRAP_SERVERS = "kafka.bq.bootstrap.servers";
  String KAFKA_BQ_TOPIC = "kafka.bq.topic";
  String KAFKA_BQ_STARTING_OFFSET = "kafka.bq.starting.offset";
  String KAFKA_BQ_AWAIT_TERMINATION_TIMEOUT = "kafka.bq.await.termination.timeout";
  String KAFKA_BQ_FAIL_ON_DATA_LOSS = "kafka.bq.fail.on.dataloss";
  String KAFKA_BQ_DATASET = "kafka.bq.dataset";
  String KAFKA_BQ_TABLE = "kafka.bq.table";
  String KAFKA_BQ_TEMP_GCS_BUCKET = "kafka.bq.temp.gcs.bucket";
  String KAFKA_BQ_STREAM_OUTPUT_MODE = "kafka.bq.stream.output.mode";

  /** Common properties for all Kafka Based Templates * */
  String KAFKA_MESSAGE_FORMAT = "kafka.message.format";

  String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  String KAFKA_TOPIC = "kafka.topic";
  String KAFKA_STARTING_OFFSET = "kafka.starting.offset";
  String KAFKA_SCHEMA_URL = "kafka.schema.url";

  /** Kafka To GCS properties */
  String KAFKA_GCS_OUTPUT_LOCATION = "kafka.gcs.output.location";

  String KAFKA_GCS_OUTPUT_FORMAT = "kafka.gcs.output.format";
  String KAFKA_GCS_OUTPUT_MODE = "kafka.gcs.output.mode";
  String KAFKA_GCS_AWAIT_TERMINATION_TIMEOUT = "kafka.gcs.await.termination.timeout.ms";

  /** SnowflakeToGCS properties */
  String SNOWFLAKE_GCS_SFURL = "snowflake.gcs.sfurl";

  String SNOWFLAKE_GCS_SFUSER = "snowflake.gcs.sfuser";
  String SNOWFLAKE_GCS_SFPASSWORD = "snowflake.gcs.sfpassword";
  String SNOWFLAKE_GCS_SFDATABASE = "snowflake.gcs.sfdatabase";
  String SNOWFLAKE_GCS_SFSCHEMA = "snowflake.gcs.sfschema";
  String SNOWFLAKE_GCS_SFWAREHOUSE = "snowflake.gcs.sfwarehouse";
  String SNOWFLAKE_GCS_AUTOPUSHDOWN = "snowflake.gcs.autopushdown";
  String SNOWFLAKE_GCS_TABLE = "snowflake.gcs.table";
  String SNOWFLAKE_GCS_QUERY = "snowflake.gcs.query";
  String SNOWFLAKE_GCS_OUTPUT_LOCATION = "snowflake.gcs.output.location";
  String SNOWFLAKE_GCS_OUTPUT_FORMAT = "snowflake.gcs.output.format";
  String SNOWFLAKE_GCS_OUTPUT_MODE = "snowflake.gcs.output.mode";
  String SNOWFLAKE_GCS_OUTPUT_PARTITION_COLUMN = "snowflake.gcs.output.partitionColumn";

  /** MongoToGCS properties */
  String MONGO_GCS_OUTPUT_LOCATION = "mongo.gcs.output.location";

  String MONGO_GCS_OUTPUT_FORMAT = "mongo.gcs.output.format";
  String MONGO_GCS_OUTPUT_MODE = "mongo.gcs.output.mode";
  String MONGO_GCS_INPUT_URI = "mongo.gcs.input.uri";
  String MONGO_GCS_INPUT_DATABASE = "mongo.gcs.input.database";
  String MONGO_GCS_INPUT_COLLECTION = "mongo.gcs.input.collection";

  /** JDBCToJDBC properties */
  String JDBCTOJDBC_INPUT_URL = "jdbctojdbc.input.url";

  String JDBCTOJDBC_INPUT_DRIVER = "jdbctojdbc.input.driver";
  String JDBCTOJDBC_INPUT_TABLE = "jdbctojdbc.input.table";
  String JDBCTOJDBC_INPUT_FETCHSIZE = "jdbctojdbc.input.fetchsize";
  String JDBCTOJDBC_INPUT_PARTITIONCOLUMN = "jdbctojdbc.input.partitioncolumn";
  String JDBCTOJDBC_INPUT_LOWERBOUND = "jdbctojdbc.input.lowerbound";
  String JDBCTOJDBC_INPUT_UPPERBOUND = "jdbctojdbc.input.upperbound";
  String JDBCTOJDBC_NUMPARTITIONS = "jdbctojdbc.numpartitions";
  String JDBCTOJDBC_OUTPUT_URL = "jdbctojdbc.output.url";
  String JDBCTOJDBC_OUTPUT_DRIVER = "jdbctojdbc.output.driver";
  String JDBCTOJDBC_OUTPUT_TABLE = "jdbctojdbc.output.table";
  String JDBCTOJDBC_OUTPUT_CREATE_TABLE_OPTION = "jdbctojdbc.output.create.table.option";
  String JDBCTOJDBC_OUTPUT_MODE = "jdbctojdbc.output.mode";
  String JDBCTOJDBC_OUTPUT_BATCH_SIZE = "jdbctojdbc.output.batch.size";
  String JDBCTOJDBC_SESSION_INIT_STATEMENT = "jdbctojdbc.sessioninitstatement";
  String JDBCTOJDBC_OUTPUT_PRIMARY_KEY = "jdbctojdbc.output.primary.key";
  String JDBCTOJDBC_TEMP_VIEW_NAME = "jdbctojdbc.temp.view.name";
  String JDBCTOJDBC_SQL_QUERY = "jdbctojdbc.sql.query";

  /** TextToBigquery properties */
  String TEXT_BIGQUERY_INPUT_LOCATION = "text.bigquery.input.location";

  String TEXT_BIGQUERY_INPUT_COMPRESSION = "text.bigquery.input.compression";
  String TEXT_BIGQUERY_INPUT_DELIMITER = "text.bigquery.input.delimiter";
  String TEXT_BIGQUERY_OUTPUT_DATASET = "text.bigquery.output.dataset";
  String TEXT_BIGQUERY_OUTPUT_TABLE = "text.bigquery.output.table";
  String TEXT_BIGQUERY_OUTPUT_MODE = "text.bigquery.output.mode";
  String TEXT_BIGQUERY_TEMP_BUCKET = "text.bigquery.temp.bucket";
  String TEXT_BIGQUERY_TEMP_TABLE = "text.bigquery.temp.table";
  String TEXT_BIGQUERY_TEMP_QUERY = "text.bigquery.temp.query";

  String TEXT_BIGQUERY_COMPRESSION_OPTION = "compression";
  String TEXT_BIGQUERY_DELIMITER_OPTION = "delimiter";
  String TEXT_BIGQUERY_INFERSCHEMA_OPTION = "inferSchema";
  String TEXT_BIGQUERY_HEADER_OPTION = "header";

  /** Kafka To BQ - Direct stream properties */
  String KAFKA_BQ_CONSUMER_GROUP_ID = "kafka.bq.consumer.group.id";

  String KAFKA_BQ_BATCH_INTERVAL = "kafka.bq.batch.interval";

  /** Kafka To GCS - Direct stream properties */
  String KAFKA_GCS_CONSUMER_GROUP_ID = "kafka.gcs.consumer.group.id";

  String KAFKA_GCS_BATCH_INTERVAL = "kafka.gcs.batch.interval";
  String KAFKA_GCS_WRITE_MODE = "kafka.gcs.write.mode";
}
