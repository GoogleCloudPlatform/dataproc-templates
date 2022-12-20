# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Common
PROJECT_ID_PROP = "project.id"

# Data
INPUT_DELIMITER = "delimiter"
INPUT_COMPRESSION = "compression"
COMPRESSION_BZIP2 = "bzip2"
COMPRESSION_GZIP = "gzip"
COMPRESSION_DEFLATE = "deflate"
COMPRESSION_LZ4 = "lz4"
COMPRESSION_NONE = "None"
HEADER = "header"
INFER_SCHEMA = "inferSchema"
FORMAT_JSON = "json"
FORMAT_CSV = "csv"
FORMAT_TXT = "txt"
FORMAT_AVRO = "avro"
FORMAT_PRQT = "parquet"
FORMAT_AVRO_EXTD = "com.databricks.spark.avro"
FORMAT_BIGQUERY = "com.google.cloud.spark.bigquery"
FORMAT_JDBC = "jdbc"
FORMAT_REDSHIFT = "io.github.spark_redshift_community.spark.redshift"
JDBC_URL = "url"
JDBC_TABLE = "dbtable"
JDBC_DRIVER = "driver"
JDBC_FETCHSIZE = "fetchsize"
JDBC_BATCH_SIZE = "batchsize"
JDBC_PARTITIONCOLUMN = "partitionColumn"
JDBC_LOWERBOUND = "lowerBound"
JDBC_UPPERBOUND = "upperBound"
JDBC_NUMPARTITIONS = "numPartitions"
JDBC_CREATE_TABLE_OPTIONS = "createTableOptions"
FORMAT_HBASE = "org.apache.hadoop.hbase.spark"
TABLE = "table"
TEMP_GCS_BUCKET="temporaryGcsBucket"
MONGO_URL = "spark.mongodb.output.uri"
MONGO_INPUT_URI = "spark.mongodb.input.uri"
MONGO_DATABASE = "database"
MONGO_COLLECTION = "collection"
FORMAT_MONGO = "com.mongodb.spark.sql.DefaultSource"
MONGO_DEFAULT_BATCH_SIZE = 512
MONGO_BATCH_SIZE = "maxBatchSize"
FORMAT_SNOWFLAKE = "net.snowflake.spark.snowflake"
REDSHIFT_TEMPDIR = "tempdir"
REDSHIFT_IAMROLE = "aws_iam_role"
AWS_S3ACCESSKEY = "fs.s3a.access.key"
AWS_S3SECRETKEY = "fs.s3a.secret.key"
SQL_EXTENSION= "spark.sql.extensions"
CASSANDRA_EXTENSION= "com.datastax.spark.connector.CassandraSparkExtensions"
CASSANDRA_CATALOG= "com.datastax.spark.connector.datasource.CassandraCatalog"

# Output mode
OUTPUT_MODE_OVERWRITE = "overwrite"
OUTPUT_MODE_APPEND = "append"
OUTPUT_MODE_IGNORE = "ignore"
OUTPUT_MODE_ERRORIFEXISTS = "errorifexists"

# GCS to BigQuery
GCS_BQ_INPUT_LOCATION = "gcs.bigquery.input.location"
GCS_BQ_INPUT_FORMAT = "gcs.bigquery.input.format"
GCS_BQ_OUTPUT_DATASET = "gcs.bigquery.output.dataset"
GCS_BQ_OUTPUT_TABLE = "gcs.bigquery.output.table"
GCS_BQ_OUTPUT_MODE = "gcs.bigquery.output.mode"
GCS_BQ_TEMP_BUCKET = "temporaryGcsBucket"
GCS_BQ_LD_TEMP_BUCKET_NAME = "gcs.bigquery.temp.bucket.name"

# GCS to JDBC
GCS_JDBC_INPUT_LOCATION = "gcs.jdbc.input.location"
GCS_JDBC_INPUT_FORMAT = "gcs.jdbc.input.format"
GCS_JDBC_OUTPUT_TABLE = "gcs.jdbc.output.table"
GCS_JDBC_OUTPUT_MODE = "gcs.jdbc.output.mode"
GCS_JDBC_OUTPUT_URL = "gcs.jdbc.output.url"
GCS_JDBC_OUTPUT_DRIVER = "gcs.jdbc.output.driver"
GCS_JDBC_BATCH_SIZE = "gcs.jdbc.batch.size"

#GCS to Mongo
GCS_MONGO_INPUT_LOCATION = "gcs.mongo.input.location"
GCS_MONGO_INPUT_FORMAT = "gcs.mongo.input.format"
GCS_MONGO_OUTPUT_URI = "gcs.mongo.output.uri"
GCS_MONGO_OUTPUT_DATABASE = "gcs.mongo.output.database"
GCS_MONGO_OUTPUT_COLLECTION = "gcs.mongo.output.collection"
GCS_MONGO_OUTPUT_MODE = "gcs.mongo.output.mode"
GCS_MONGO_BATCH_SIZE = "gcs.mongo.batch.size"

# Mongo to GCS
MONGO_GCS_OUTPUT_LOCATION = "mongo.gcs.output.location"
MONGO_GCS_OUTPUT_FORMAT = "mongo.gcs.output.format"
MONGO_GCS_OUTPUT_MODE = "mongo.gcs.output.mode"
MONGO_GCS_INPUT_URI = "mongo.gcs.input.uri"
MONGO_GCS_INPUT_DATABASE = "mongo.gcs.input.database"
MONGO_GCS_INPUT_COLLECTION = "mongo.gcs.input.collection"

#Cassandra to BQ
CASSANDRA_TO_BQ_INPUT_TABLE = "cassandratobq.input.table"
CASSANDRA_TO_BQ_INPUT_HOST = "cassandratobq.input.host"
CASSANDRA_TO_BQ_BIGQUERY_LOCATION = "cassandratobq.bigquery.location"
CASSANDRA_TO_BQ_WRITE_MODE = "cassandratobq.output.mode"
CASSANDRA_TO_BQ_TEMP_LOCATION = "cassandratobq.temp.gcs.location"
CASSANDRA_TO_BQ_QUERY = "cassandratobq.input.query"
CASSANDRA_TO_BQ_CATALOG = "cassandratobq.input.catalog.name"
CASSANDRA_TO_BQ_INPUT_KEYSPACE = "cassandratobq.input.keyspace"

# GCS to BigTable
GCS_BT_INPUT_LOCATION = "gcs.bigtable.input.location"
GCS_BT_INPUT_FORMAT = "gcs.bigtable.input.format"
GCS_BT_HBASE_CATALOG_JSON = "gcs.bigtable.hbase.catalog.json"

# BigQuery to GCS
BQ_GCS_INPUT_TABLE = "bigquery.gcs.input.table"
BQ_GCS_OUTPUT_FORMAT = "bigquery.gcs.output.format"
BQ_GCS_OUTPUT_MODE = "bigquery.gcs.output.mode"
BQ_GCS_OUTPUT_LOCATION = "bigquery.gcs.output.location"

# GCS To GCS with transformations
GCS_TO_GCS_INPUT_LOCATION = "gcs.to.gcs.input.location"
GCS_TO_GCS_INPUT_FORMAT = "gcs.to.gcs.input.format"
GCS_TO_GCS_TEMP_VIEW_NAME = "gcs.to.gcs.temp.view.name"
GCS_TO_GCS_SQL_QUERY = "gcs.to.gcs.sql.query"
GCS_TO_GCS_OUTPUT_FORMAT = "gcs.to.gcs.output.format"
GCS_TO_GCS_OUTPUT_MODE = "gcs.to.gcs.output.mode"
GCS_TO_GCS_OUTPUT_PARTITION_COLUMN = "gcs.to.gcs.output.partition.column"
GCS_TO_GCS_OUTPUT_LOCATION = "gcs.to.gcs.output.location"

# Hive to BigQuery
HIVE_BQ_OUTPUT_MODE = "hive.bigquery.output.mode"
HIVE_BQ_LD_TEMP_BUCKET_NAME = "hive.bigquery.temp.bucket.name"
HIVE_BQ_OUTPUT_DATASET = "hive.bigquery.output.dataset"
HIVE_BQ_OUTPUT_TABLE = "hive.bigquery.output.table"
HIVE_BQ_INPUT_DATABASE = "hive.bigquery.input.database"
HIVE_BQ_INPUT_TABLE = "hive.bigquery.input.table"
HIVE_BQ_TEMP_VIEW_NAME = "hive.bigquery.temp.view.name"
HIVE_BQ_SQL_QUERY = "hive.bigquery.sql.query"

# Hive to GCS
HIVE_GCS_INPUT_DATABASE="hive.gcs.input.database"
HIVE_GCS_INPUT_TABLE = "hive.gcs.input.table"
HIVE_GCS_OUTPUT_LOCATION = "hive.gcs.output.location"
HIVE_GCS_OUTPUT_FORMAT = "hive.gcs.output.format"
HIVE_GCS_OUTPUT_MODE = "hive.gcs.output.mode"
HIVE_GCS_TEMP_VIEW_NAME = "hive.gcs.temp.view.name"
HIVE_GCS_SQL_QUERY = "hive.gcs.sql.query"

# Text to BigQuery
TEXT_INPUT_COMPRESSION = "text.bigquery.input.compression"
TEXT_INPUT_DELIMITER = "text.bigquery.input.delimiter"
TEXT_BQ_INPUT_LOCATION = "text.bigquery.input.location"
TEXT_BQ_OUTPUT_DATASET = "text.bigquery.output.dataset"
TEXT_BQ_OUTPUT_TABLE = "text.bigquery.output.table"
TEXT_BQ_OUTPUT_MODE = "text.bigquery.output.mode"
TEXT_BQ_TEMP_BUCKET = "temporaryGcsBucket"
TEXT_BQ_LD_TEMP_BUCKET_NAME = "text.bigquery.temp.bucket.name"
TEXT_BQ_INPUT_INFERSCHEMA = "text.bigquery.input.inferschema"

# Hbase to GCS
HBASE_GCS_OUTPUT_LOCATION = "hbase.gcs.output.location"
HBASE_GCS_OUTPUT_FORMAT = "hbase.gcs.output.format"
HBASE_GCS_OUTPUT_MODE = "hbase.gcs.output.mode"
HBASE_GCS_CATALOG_JSON = "hbase.gcs.catalog.json"

# JDBC to JDBC
JDBCTOJDBC_INPUT_URL = "jdbctojdbc.input.url"
JDBCTOJDBC_INPUT_DRIVER = "jdbctojdbc.input.driver"
JDBCTOJDBC_INPUT_TABLE = "jdbctojdbc.input.table"
JDBCTOJDBC_INPUT_FETCHSIZE = "jdbctojdbc.input.fetchsize"
JDBCTOJDBC_INPUT_PARTITIONCOLUMN = "jdbctojdbc.input.partitioncolumn"
JDBCTOJDBC_INPUT_LOWERBOUND = "jdbctojdbc.input.lowerbound"
JDBCTOJDBC_INPUT_UPPERBOUND = "jdbctojdbc.input.upperbound"
JDBCTOJDBC_NUMPARTITIONS = "jdbctojdbc.numpartitions"
JDBCTOJDBC_OUTPUT_URL = "jdbctojdbc.output.url"
JDBCTOJDBC_OUTPUT_DRIVER = "jdbctojdbc.output.driver"
JDBCTOJDBC_OUTPUT_TABLE = "jdbctojdbc.output.table"
JDBCTOJDBC_OUTPUT_CREATE_TABLE_OPTION = "jdbctojdbc.output.create_table.option"
JDBCTOJDBC_OUTPUT_MODE = "jdbctojdbc.output.mode"
JDBCTOJDBC_OUTPUT_BATCH_SIZE = "jdbctojdbc.output.batch.size"
JDBCTOJDBC_TEMP_VIEW_NAME = "jdbctojdbc.temp.view.name"
JDBCTOJDBC_SQL_QUERY = "jdbctojdbc.sql.query"

# JDBC to GCS
JDBCTOGCS_INPUT_URL = "jdbctogcs.input.url"
JDBCTOGCS_INPUT_DRIVER = "jdbctogcs.input.driver"
JDBCTOGCS_INPUT_TABLE = "jdbctogcs.input.table"
JDBCTOGCS_INPUT_FETCHSIZE = "jdbctogcs.input.fetchsize"
JDBCTOGCS_INPUT_PARTITIONCOLUMN = "jdbctogcs.input.partitioncolumn"
JDBCTOGCS_INPUT_LOWERBOUND = "jdbctogcs.input.lowerbound"
JDBCTOGCS_INPUT_UPPERBOUND = "jdbctogcs.input.upperbound"
JDBCTOGCS_NUMPARTITIONS = "jdbctogcs.numpartitions"
JDBCTOGCS_OUTPUT_LOCATION = "jdbctogcs.output.location"
JDBCTOGCS_OUTPUT_FORMAT = "jdbctogcs.output.format"
JDBCTOGCS_OUTPUT_MODE = "jdbctogcs.output.mode"
JDBCTOGCS_OUTPUT_PARTITIONCOLUMN = "jdbctogcs.output.partitioncolumn"
JDBCTOGCS_TEMP_VIEW_NAME = "jdbctogcs.temp.view.name"
JDBCTOGCS_SQL_QUERY = "jdbctogcs.sql.query"

# JDBC to BigQuery
JDBC_BQ_INPUT_URL = "jdbc.bigquery.input.url"
JDBC_BQ_INPUT_DRIVER = "jdbc.bigquery.input.driver"
JDBC_BQ_INPUT_TABLE = "jdbc.bigquery.input.table"
JDBC_BQ_INPUT_FETCHSIZE = "jdbc.bigquery.input.fetchsize"
JDBC_BQ_INPUT_PARTITIONCOLUMN = "jdbc.bigquery.input.partitioncolumn"
JDBC_BQ_INPUT_LOWERBOUND = "jdbc.bigquery.input.lowerbound"
JDBC_BQ_INPUT_UPPERBOUND = "jdbc.bigquery.input.upperbound"
JDBC_BQ_NUMPARTITIONS = "jdbc.bigquery.numpartitions"
JDBC_BQ_OUTPUT_MODE = "jdbc.bigquery.output.mode"
JDBC_BQ_OUTPUT_DATASET = "jdbc.bigquery.output.dataset"
JDBC_BQ_OUTPUT_TABLE = "jdbc.bigquery.output.table"
JDBC_BQ_OUTPUT_MODE = "jdbc.bigquery.output.mode"
JDBC_BQ_TEMP_BUCKET = "temporaryGcsBucket"
JDBC_BQ_LD_TEMP_BUCKET_NAME = "jdbc.bigquery.temp.bucket.name"

#REDSHIFT to GCS
REDSHIFTTOGCS_INPUT_URL = "redshifttogcs.input.url"
REDSHIFTTOGCS_S3_TEMPDIR = "redshifttogcs.s3.tempdir"
REDSHIFTTOGCS_INPUT_TABLE = "redshifttogcs.input.table"
REDSHIFTTOGCS_IAM_ROLEARN = "redshifttogcs.iam.rolearn"
REDSHIFTTOGCS_S3_ACCESSKEY = "redshifttogcs.s3.accesskey"
REDSHIFTTOGCS_S3_SECRETKEY = "redshifttogcs.s3.secretkey"
REDSHIFTTOGCS_OUTPUT_LOCATION = "redshifttogcs.output.location"
REDSHIFTTOGCS_OUTPUT_FORMAT = "redshifttogcs.output.format"
REDSHIFTTOGCS_OUTPUT_MODE = "redshifttogcs.output.mode"
REDSHIFTTOGCS_OUTPUT_PARTITIONCOLUMN = "redshifttogcs.output.partitioncolumn"

# Snowflake To GCS
SNOWFLAKE_TO_GCS_SF_URL = "snowflake.to.gcs.sf.url"
SNOWFLAKE_TO_GCS_SF_USER = "snowflake.to.gcs.sf.user"
SNOWFLAKE_TO_GCS_SF_PASSWORD = "snowflake.to.gcs.sf.password"
SNOWFLAKE_TO_GCS_SF_DATABASE = "snowflake.to.gcs.sf.database"
SNOWFLAKE_TO_GCS_SF_SCHEMA = "snowflake.to.gcs.sf.schema"
SNOWFLAKE_TO_GCS_SF_WAREHOUSE = "snowflake.to.gcs.sf.warehouse"
SNOWFLAKE_TO_GCS_SF_AUTOPUSHDOWN = "snowflake.to.gcs.sf.autopushdown"
SNOWFLAKE_TO_GCS_SF_TABLE = "snowflake.to.gcs.sf.table"
SNOWFLAKE_TO_GCS_SF_QUERY = "snowflake.to.gcs.sf.query"
SNOWFLAKE_TO_GCS_OUTPUT_LOCATION = "snowflake.to.gcs.output.location"
SNOWFLAKE_TO_GCS_OUTPUT_MODE = "snowflake.to.gcs.output.mode"
SNOWFLAKE_TO_GCS_OUTPUT_FORMAT = "snowflake.to.gcs.output.format"
SNOWFLAKE_TO_GCS_PARTITION_COLUMN = "snowflake.to.gcs.partition.column"

