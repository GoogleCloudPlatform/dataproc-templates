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
TEXT_HEADER = "header"
TEXT_INFER_SCHEMA = "inferSchema"
INPUT_DELIMITER = "delimiter"
INPUT_COMPRESSION = "compression"
COMPRESSION_BZIP2 = "bzip2"
COMPRESSION_GZIP = "gzip"
COMPRESSION_DEFLATE = "deflate"
COMPRESSION_LZ4 = "lz4"
COMPRESSION_NONE = "None"
DELIMITER_COMMA = ","
DELIMITER_PIPE = "|"
DELIMITER_SLASH = "/"
DELIMITER_SEMICOLON =";"
CSV_HEADER = "header"
CSV_INFER_SCHEMA = "inferSchema"
FORMAT_JSON = "json"
FORMAT_CSV = "csv"
FORMAT_TXT = "txt"
FORMAT_AVRO = "avro"
FORMAT_PRQT = "parquet"
FORMAT_AVRO_EXTD = "com.databricks.spark.avro"
FORMAT_BIGQUERY = "com.google.cloud.spark.bigquery"
TABLE = "table"
TEMP_GCS_BUCKET="temporaryGcsBucket"

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

# BigQuery to GCS
BQ_GCS_INPUT_TABLE = "bigquery.gcs.input.table"
BQ_GCS_OUTPUT_FORMAT = "bigquery.gcs.output.format"
BQ_GCS_OUTPUT_MODE = "bigquery.gcs.output.mode"
BQ_GCS_OUTPUT_LOCATION = "bigquery.gcs.output.location"

# Hive to BigQuery
HIVE_BQ_OUTPUT_MODE = "hive.bigquery.output.mode"
HIVE_BQ_LD_TEMP_BUCKET_NAME = "hive.bigquery.temp.bucket.name"
HIVE_BQ_OUTPUT_DATASET = "hive.bigquery.output.dataset"
HIVE_BQ_OUTPUT_TABLE = "hive.bigquery.output.table"
HIVE_BQ_INPUT_DATABASE="hive.bigquery.input.database"
HIVE_BQ_INPUT_TABLE="hive.bigquery.input.table"

# Hive to GCS
HIVE_GCS_INPUT_DATABASE="hive.gcs.input.database"
HIVE_GCS_INPUT_TABLE="hive.gcs.input.table"
HIVE_GCS_OUTPUT_LOCATION = "hive.gcs.output.location"
HIVE_GCS_OUTPUT_FORMAT = "hive.gcs.output.format"
HIVE_GCS_OUTPUT_MODE = "hive.gcs.output.mode"

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
