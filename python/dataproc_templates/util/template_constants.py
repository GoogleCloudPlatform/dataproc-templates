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
FORMAT_JSON = "json"
FORMAT_CSV = "csv"
FORMAT_DELTA = "delta"
FORMAT_TXT = "txt"
FORMAT_AVRO = "avro"
FORMAT_PRQT = "parquet"
FORMAT_AVRO_EXTD = "com.databricks.spark.avro"
FORMAT_BIGQUERY = "com.google.cloud.spark.bigquery"
FORMAT_JDBC = "jdbc"
FORMAT_REDSHIFT = "io.github.spark_redshift_community.spark.redshift"
JDBC_URL = "url"
JDBC_TABLE = "dbtable"
JDBC_QUERY = "query"
JDBC_DRIVER = "driver"
JDBC_FETCHSIZE = "fetchsize"
JDBC_BATCH_SIZE = "batchsize"
JDBC_PARTITIONCOLUMN = "partitionColumn"
JDBC_LOWERBOUND = "lowerBound"
JDBC_UPPERBOUND = "upperBound"
JDBC_NUMPARTITIONS = "numPartitions"
JDBC_SESSIONINITSTATEMENT = "sessionInitStatement"
JDBC_CREATE_TABLE_OPTIONS = "createTableOptions"
CSV_CHARTOESCAPEQUOTEESCAPING = "charToEscapeQuoteEscaping"
CSV_COLUMNNAMEOFCORRUPTRECORD = "columnNameOfCorruptRecord"
CSV_COMMENT = "comment"
CSV_DATEFORMAT = "dateFormat"
CSV_EMPTYVALUE = "emptyValue"
CSV_ENCODING = "encoding"
CSV_ENFORCESCHEMA = "enforceSchema"
CSV_ESCAPE = "escape"
CSV_ESCAPEQUOTES = "escapeQuotes"
CSV_HEADER = "header"
CSV_IGNORELEADINGWHITESPACE = "ignoreLeadingWhiteSpace"
CSV_IGNORETRAILINGWHITESPACE = "ignoreTrailingWhiteSpace"
CSV_INFER_SCHEMA = "inferSchema"
CSV_LINESEP = "lineSep"
CSV_LOCALE = "locale"
CSV_MAXCHARSPERCOLUMN = "maxCharsPerColumn"
CSV_MAXCOLUMNS = "maxColumns"
CSV_MODE = "mode"
CSV_MULTILINE = "multiLine"
CSV_NANVALUE = "nanValue"
CSV_NULLVALUE = "nullValue"
CSV_NEGATIVEINF = "negativeInf"
CSV_POSITIVEINF = "positiveInf"
CSV_QUOTE = "quote"
CSV_QUOTEALL = "quoteAll"
CSV_SAMPLINGRATIO = "samplingRatio"
CSV_SEP = "sep"
CSV_TIMESTAMPFORMAT = "timestampFormat"
CSV_TIMESTAMPNTZFORMAT = "timestampNTZFormat"
CSV_UNESCAPEDQUOTEHANDLING = "unescapedQuoteHandling"
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
AWS_S3ENDPOINT = "fs.s3a.endpoint"
SQL_EXTENSION= "spark.sql.extensions"
CASSANDRA_EXTENSION= "com.datastax.spark.connector.CassandraSparkExtensions"
CASSANDRA_CATALOG= "com.datastax.spark.connector.datasource.CassandraCatalog"
FORMAT_PUBSUBLITE = "pubsublite"

OPTION_DEFAULT = "default"
OPTION_HELP = "help"
# At the moment this is just a map of CSV related options but it will be expanded as required for other uses.
SPARK_OPTIONS = {
    CSV_CHARTOESCAPEQUOTEESCAPING: {OPTION_HELP: ""},
    CSV_COLUMNNAMEOFCORRUPTRECORD: {OPTION_HELP: ""},
    CSV_COMMENT: {OPTION_HELP: ""},
    CSV_DATEFORMAT: {OPTION_HELP: ""},
    CSV_EMPTYVALUE: {OPTION_HELP: ""},
    CSV_ENCODING: {OPTION_HELP: ""},
    CSV_ENFORCESCHEMA: {OPTION_HELP: ""},
    CSV_ESCAPE: {OPTION_HELP: ""},
    CSV_ESCAPEQUOTES: {OPTION_HELP: ""},
    CSV_HEADER: {OPTION_DEFAULT: 'true',
                 OPTION_HELP: "Uses the first line of CSV file as names of columns. Defaults to True"},
    CSV_IGNORELEADINGWHITESPACE: {OPTION_HELP: ""},
    CSV_IGNORETRAILINGWHITESPACE: {OPTION_HELP: ""},
    CSV_INFER_SCHEMA: {OPTION_DEFAULT: "true",
                       OPTION_HELP: "Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True"},
    CSV_LINESEP: {OPTION_HELP: "Defines the line separator that should be used for parsing"},
    CSV_LOCALE: {OPTION_HELP: "Sets a locale as language tag in IETF BCP 47 format"},
    CSV_MAXCHARSPERCOLUMN: {OPTION_HELP: ""},
    CSV_MAXCOLUMNS: {OPTION_HELP: ""},
    CSV_MODE: {OPTION_HELP: "Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST"},
    CSV_MULTILINE: {OPTION_HELP: ""},
    CSV_NANVALUE: {OPTION_HELP: ""},
    CSV_NULLVALUE: {OPTION_HELP: ""},
    CSV_NEGATIVEINF: {OPTION_HELP: ""},
    CSV_POSITIVEINF: {OPTION_HELP: ""},
    CSV_QUOTE: {OPTION_HELP: ""},
    CSV_QUOTEALL: {OPTION_HELP: ""},
    CSV_SAMPLINGRATIO: {OPTION_HELP: ""},
    CSV_SEP: {OPTION_HELP: "Sets a separator for each field and value. This separator can be one or more characters"},
    CSV_TIMESTAMPFORMAT: {OPTION_HELP: "Sets the string that indicates a timestamp format"},
    CSV_TIMESTAMPNTZFORMAT: {OPTION_HELP: "Sets the string that indicates a timestamp without timezone format"},
    CSV_UNESCAPEDQUOTEHANDLING: {OPTION_HELP: "Defines how the CsvParser will handle values with unescaped quotes"},
}

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
GCS_BQ_INPUT_CHARTOESCAPEQUOTEESCAPING = "gcs.bigquery.input.chartoescapequoteescaping"
GCS_BQ_INPUT_COLUMNNAMEOFCORRUPTRECORD = "gcs.bigquery.input.columnnameofcorruptrecord"
GCS_BQ_INPUT_COMMENT = "gcs.bigquery.input.comment"
GCS_BQ_INPUT_DATEFORMAT = "gcs.bigquery.input.dateformat"
GCS_BQ_INPUT_EMPTYVALUE = "gcs.bigquery.input.emptyvalue"
GCS_BQ_INPUT_ENCODING = "gcs.bigquery.input.encoding"
GCS_BQ_INPUT_ENFORCESCHEMA = "gcs.bigquery.input.enforceschema"
GCS_BQ_INPUT_ESCAPE = "gcs.bigquery.input.escape"
GCS_BQ_INPUT_HEADER = "gcs.bigquery.input.header"
GCS_BQ_INPUT_IGNORELEADINGWHITESPACE = "gcs.bigquery.input.ignoreleadingwhitespace"
GCS_BQ_INPUT_IGNORETRAILINGWHITESPACE = "gcs.bigquery.input.ignoretrailingwhitespace"
GCS_BQ_INPUT_INFER_SCHEMA = "gcs.bigquery.input.inferschema"
GCS_BQ_INPUT_LINESEP = "gcs.bigquery.input.linesep"
GCS_BQ_INPUT_LOCALE = "gcs.bigquery.input.locale"
GCS_BQ_INPUT_MAXCHARSPERCOLUMN = "gcs.bigquery.input.maxcharspercolumn"
GCS_BQ_INPUT_MAXCOLUMNS = "gcs.bigquery.input.maxcolumns"
GCS_BQ_INPUT_MODE = "gcs.bigquery.input.mode"
GCS_BQ_INPUT_MULTILINE = "gcs.bigquery.input.multiline"
GCS_BQ_INPUT_NANVALUE = "gcs.bigquery.input.nanvalue"
GCS_BQ_INPUT_NULLVALUE = "gcs.bigquery.input.nullvalue"
GCS_BQ_INPUT_NEGATIVEINF = "gcs.bigquery.input.negativeinf"
GCS_BQ_INPUT_POSITIVEINF = "gcs.bigquery.input.positiveinf"
GCS_BQ_INPUT_QUOTE = "gcs.bigquery.input.quote"
GCS_BQ_INPUT_SAMPLINGRATIO = "gcs.bigquery.input.samplingratio"
GCS_BQ_INPUT_SEP = "gcs.bigquery.input.sep"
GCS_BQ_INPUT_TIMESTAMPFORMAT = "gcs.bigquery.input.timestampformat"
GCS_BQ_INPUT_TIMESTAMPNTZFORMAT = "gcs.bigquery.input.timestampntzformat"
GCS_BQ_INPUT_UNESCAPEDQUOTEHANDLING = "gcs.bigquery.input.unescapedquotehandling"
# Template options linked to JDBC option names.
GCS_BQ_INPUT_SPARK_OPTIONS = {
    GCS_BQ_INPUT_CHARTOESCAPEQUOTEESCAPING: CSV_CHARTOESCAPEQUOTEESCAPING,
    GCS_BQ_INPUT_COLUMNNAMEOFCORRUPTRECORD: CSV_COLUMNNAMEOFCORRUPTRECORD,
    GCS_BQ_INPUT_COMMENT: CSV_COMMENT,
    GCS_BQ_INPUT_DATEFORMAT: CSV_DATEFORMAT,
    GCS_BQ_INPUT_EMPTYVALUE: CSV_EMPTYVALUE,
    GCS_BQ_INPUT_ENCODING: CSV_ENCODING,
    GCS_BQ_INPUT_ENFORCESCHEMA: CSV_ENFORCESCHEMA,
    GCS_BQ_INPUT_ESCAPE: CSV_ESCAPE,
    GCS_BQ_INPUT_HEADER: CSV_HEADER,
    GCS_BQ_INPUT_IGNORELEADINGWHITESPACE: CSV_IGNORELEADINGWHITESPACE,
    GCS_BQ_INPUT_IGNORETRAILINGWHITESPACE: CSV_IGNORETRAILINGWHITESPACE,
    GCS_BQ_INPUT_INFER_SCHEMA: CSV_INFER_SCHEMA,
    GCS_BQ_INPUT_LINESEP: CSV_LINESEP,
    GCS_BQ_INPUT_LOCALE: CSV_LOCALE,
    GCS_BQ_INPUT_MAXCHARSPERCOLUMN: CSV_MAXCHARSPERCOLUMN,
    GCS_BQ_INPUT_MAXCOLUMNS: CSV_MAXCOLUMNS,
    GCS_BQ_INPUT_MODE: CSV_MODE,
    GCS_BQ_INPUT_MULTILINE: CSV_MULTILINE,
    GCS_BQ_INPUT_NANVALUE: CSV_NANVALUE,
    GCS_BQ_INPUT_NULLVALUE: CSV_NULLVALUE,
    GCS_BQ_INPUT_NEGATIVEINF: CSV_NEGATIVEINF,
    GCS_BQ_INPUT_POSITIVEINF: CSV_POSITIVEINF,
    GCS_BQ_INPUT_QUOTE: CSV_QUOTE,
    GCS_BQ_INPUT_SAMPLINGRATIO: CSV_SAMPLINGRATIO,
    GCS_BQ_INPUT_SEP: CSV_SEP,
    GCS_BQ_INPUT_TIMESTAMPFORMAT: CSV_TIMESTAMPFORMAT,
    GCS_BQ_INPUT_TIMESTAMPNTZFORMAT: CSV_TIMESTAMPNTZFORMAT,
    GCS_BQ_INPUT_UNESCAPEDQUOTEHANDLING: CSV_UNESCAPEDQUOTEHANDLING,
}

# GCS to JDBC
GCS_JDBC_INPUT_LOCATION = "gcs.jdbc.input.location"
GCS_JDBC_INPUT_FORMAT = "gcs.jdbc.input.format"
GCS_JDBC_OUTPUT_TABLE = "gcs.jdbc.output.table"
GCS_JDBC_OUTPUT_MODE = "gcs.jdbc.output.mode"
GCS_JDBC_OUTPUT_URL = "gcs.jdbc.output.url"
GCS_JDBC_OUTPUT_DRIVER = "gcs.jdbc.output.driver"
GCS_JDBC_BATCH_SIZE = "gcs.jdbc.batch.size"
GCS_JDBC_NUMPARTITIONS = "gcs.jdbc.numpartitions"
GCS_JDBC_INPUT_CHARTOESCAPEQUOTEESCAPING = "gcs.jdbc.input.chartoescapequoteescaping"
GCS_JDBC_INPUT_COLUMNNAMEOFCORRUPTRECORD = "gcs.jdbc.input.columnnameofcorruptrecord"
GCS_JDBC_INPUT_COMMENT = "gcs.jdbc.input.comment"
GCS_JDBC_INPUT_DATEFORMAT = "gcs.jdbc.input.dateformat"
GCS_JDBC_INPUT_EMPTYVALUE = "gcs.jdbc.input.emptyvalue"
GCS_JDBC_INPUT_ENCODING = "gcs.jdbc.input.encoding"
GCS_JDBC_INPUT_ENFORCESCHEMA = "gcs.jdbc.input.enforceschema"
GCS_JDBC_INPUT_ESCAPE = "gcs.jdbc.input.escape"
GCS_JDBC_INPUT_HEADER = "gcs.jdbc.input.header"
GCS_JDBC_INPUT_IGNORELEADINGWHITESPACE = "gcs.jdbc.input.ignoreleadingwhitespace"
GCS_JDBC_INPUT_IGNORETRAILINGWHITESPACE = "gcs.jdbc.input.ignoretrailingwhitespace"
GCS_JDBC_INPUT_INFER_SCHEMA = "gcs.jdbc.input.inferschema"
GCS_JDBC_INPUT_LINESEP = "gcs.jdbc.input.linesep"
GCS_JDBC_INPUT_LOCALE = "gcs.jdbc.input.locale"
GCS_JDBC_INPUT_MAXCHARSPERCOLUMN = "gcs.jdbc.input.maxcharspercolumn"
GCS_JDBC_INPUT_MAXCOLUMNS = "gcs.jdbc.input.maxcolumns"
GCS_JDBC_INPUT_MODE = "gcs.jdbc.input.mode"
GCS_JDBC_INPUT_MULTILINE = "gcs.jdbc.input.multiline"
GCS_JDBC_INPUT_NANVALUE = "gcs.jdbc.input.nanvalue"
GCS_JDBC_INPUT_NULLVALUE = "gcs.jdbc.input.nullvalue"
GCS_JDBC_INPUT_NEGATIVEINF = "gcs.jdbc.input.negativeinf"
GCS_JDBC_INPUT_POSITIVEINF = "gcs.jdbc.input.positiveinf"
GCS_JDBC_INPUT_QUOTE = "gcs.jdbc.input.quote"
GCS_JDBC_INPUT_SAMPLINGRATIO = "gcs.jdbc.input.samplingratio"
GCS_JDBC_INPUT_SEP = "gcs.jdbc.input.sep"
GCS_JDBC_INPUT_TIMESTAMPFORMAT = "gcs.jdbc.input.timestampformat"
GCS_JDBC_INPUT_TIMESTAMPNTZFORMAT = "gcs.jdbc.input.timestampntzformat"
GCS_JDBC_INPUT_UNESCAPEDQUOTEHANDLING = "gcs.jdbc.input.unescapedquotehandling"
# Template options linked to JDBC option names.
GCS_JDBC_INPUT_SPARK_OPTIONS = {
    GCS_JDBC_INPUT_CHARTOESCAPEQUOTEESCAPING: CSV_CHARTOESCAPEQUOTEESCAPING,
    GCS_JDBC_INPUT_COLUMNNAMEOFCORRUPTRECORD: CSV_COLUMNNAMEOFCORRUPTRECORD,
    GCS_JDBC_INPUT_COMMENT: CSV_COMMENT,
    GCS_JDBC_INPUT_DATEFORMAT: CSV_DATEFORMAT,
    GCS_JDBC_INPUT_EMPTYVALUE: CSV_EMPTYVALUE,
    GCS_JDBC_INPUT_ENCODING: CSV_ENCODING,
    GCS_JDBC_INPUT_ENFORCESCHEMA: CSV_ENFORCESCHEMA,
    GCS_JDBC_INPUT_ESCAPE: CSV_ESCAPE,
    GCS_JDBC_INPUT_HEADER: CSV_HEADER,
    GCS_JDBC_INPUT_IGNORELEADINGWHITESPACE: CSV_IGNORELEADINGWHITESPACE,
    GCS_JDBC_INPUT_IGNORETRAILINGWHITESPACE: CSV_IGNORETRAILINGWHITESPACE,
    GCS_JDBC_INPUT_INFER_SCHEMA: CSV_INFER_SCHEMA,
    GCS_JDBC_INPUT_LINESEP: CSV_LINESEP,
    GCS_JDBC_INPUT_LOCALE: CSV_LOCALE,
    GCS_JDBC_INPUT_MAXCHARSPERCOLUMN: CSV_MAXCHARSPERCOLUMN,
    GCS_JDBC_INPUT_MAXCOLUMNS: CSV_MAXCOLUMNS,
    GCS_JDBC_INPUT_MODE: CSV_MODE,
    GCS_JDBC_INPUT_MULTILINE: CSV_MULTILINE,
    GCS_JDBC_INPUT_NANVALUE: CSV_NANVALUE,
    GCS_JDBC_INPUT_NULLVALUE: CSV_NULLVALUE,
    GCS_JDBC_INPUT_NEGATIVEINF: CSV_NEGATIVEINF,
    GCS_JDBC_INPUT_POSITIVEINF: CSV_POSITIVEINF,
    GCS_JDBC_INPUT_QUOTE: CSV_QUOTE,
    GCS_JDBC_INPUT_SAMPLINGRATIO: CSV_SAMPLINGRATIO,
    GCS_JDBC_INPUT_SEP: CSV_SEP,
    GCS_JDBC_INPUT_TIMESTAMPFORMAT: CSV_TIMESTAMPFORMAT,
    GCS_JDBC_INPUT_TIMESTAMPNTZFORMAT: CSV_TIMESTAMPNTZFORMAT,
    GCS_JDBC_INPUT_UNESCAPEDQUOTEHANDLING: CSV_UNESCAPEDQUOTEHANDLING,
}

#GCS to Mongo
GCS_MONGO_INPUT_LOCATION = "gcs.mongo.input.location"
GCS_MONGO_INPUT_FORMAT = "gcs.mongo.input.format"
GCS_MONGO_OUTPUT_URI = "gcs.mongo.output.uri"
GCS_MONGO_OUTPUT_DATABASE = "gcs.mongo.output.database"
GCS_MONGO_OUTPUT_COLLECTION = "gcs.mongo.output.collection"
GCS_MONGO_OUTPUT_MODE = "gcs.mongo.output.mode"
GCS_MONGO_BATCH_SIZE = "gcs.mongo.batch.size"
GCS_MONGO_INPUT_CHARTOESCAPEQUOTEESCAPING = "gcs.mongo.input.chartoescapequoteescaping"
GCS_MONGO_INPUT_COLUMNNAMEOFCORRUPTRECORD = "gcs.mongo.input.columnnameofcorruptrecord"
GCS_MONGO_INPUT_COMMENT = "gcs.mongo.input.comment"
GCS_MONGO_INPUT_DATEFORMAT = "gcs.mongo.input.dateformat"
GCS_MONGO_INPUT_EMPTYVALUE = "gcs.mongo.input.emptyvalue"
GCS_MONGO_INPUT_ENCODING = "gcs.mongo.input.encoding"
GCS_MONGO_INPUT_ENFORCESCHEMA = "gcs.mongo.input.enforceschema"
GCS_MONGO_INPUT_ESCAPE = "gcs.mongo.input.escape"
GCS_MONGO_INPUT_HEADER = "gcs.mongo.input.header"
GCS_MONGO_INPUT_IGNORELEADINGWHITESPACE = "gcs.mongo.input.ignoreleadingwhitespace"
GCS_MONGO_INPUT_IGNORETRAILINGWHITESPACE = "gcs.mongo.input.ignoretrailingwhitespace"
GCS_MONGO_INPUT_INFER_SCHEMA = "gcs.mongo.input.inferschema"
GCS_MONGO_INPUT_LINESEP = "gcs.mongo.input.linesep"
GCS_MONGO_INPUT_LOCALE = "gcs.mongo.input.locale"
GCS_MONGO_INPUT_MAXCHARSPERCOLUMN = "gcs.mongo.input.maxcharspercolumn"
GCS_MONGO_INPUT_MAXCOLUMNS = "gcs.mongo.input.maxcolumns"
GCS_MONGO_INPUT_MODE = "gcs.mongo.input.mode"
GCS_MONGO_INPUT_MULTILINE = "gcs.mongo.input.multiline"
GCS_MONGO_INPUT_NANVALUE = "gcs.mongo.input.nanvalue"
GCS_MONGO_INPUT_NULLVALUE = "gcs.mongo.input.nullvalue"
GCS_MONGO_INPUT_NEGATIVEINF = "gcs.mongo.input.negativeinf"
GCS_MONGO_INPUT_POSITIVEINF = "gcs.mongo.input.positiveinf"
GCS_MONGO_INPUT_QUOTE = "gcs.mongo.input.quote"
GCS_MONGO_INPUT_SAMPLINGRATIO = "gcs.mongo.input.samplingratio"
GCS_MONGO_INPUT_SEP = "gcs.mongo.input.sep"
GCS_MONGO_INPUT_TIMESTAMPFORMAT = "gcs.mongo.input.timestampformat"
GCS_MONGO_INPUT_TIMESTAMPNTZFORMAT = "gcs.mongo.input.timestampntzformat"
GCS_MONGO_INPUT_UNESCAPEDQUOTEHANDLING = "gcs.mongo.input.unescapedquotehandling"
# Template options linked to JDBC option names.
GCS_MONGO_INPUT_SPARK_OPTIONS = {
    GCS_MONGO_INPUT_CHARTOESCAPEQUOTEESCAPING: CSV_CHARTOESCAPEQUOTEESCAPING,
    GCS_MONGO_INPUT_COLUMNNAMEOFCORRUPTRECORD: CSV_COLUMNNAMEOFCORRUPTRECORD,
    GCS_MONGO_INPUT_COMMENT: CSV_COMMENT,
    GCS_MONGO_INPUT_DATEFORMAT: CSV_DATEFORMAT,
    GCS_MONGO_INPUT_EMPTYVALUE: CSV_EMPTYVALUE,
    GCS_MONGO_INPUT_ENCODING: CSV_ENCODING,
    GCS_MONGO_INPUT_ENFORCESCHEMA: CSV_ENFORCESCHEMA,
    GCS_MONGO_INPUT_ESCAPE: CSV_ESCAPE,
    GCS_MONGO_INPUT_HEADER: CSV_HEADER,
    GCS_MONGO_INPUT_IGNORELEADINGWHITESPACE: CSV_IGNORELEADINGWHITESPACE,
    GCS_MONGO_INPUT_IGNORETRAILINGWHITESPACE: CSV_IGNORETRAILINGWHITESPACE,
    GCS_MONGO_INPUT_INFER_SCHEMA: CSV_INFER_SCHEMA,
    GCS_MONGO_INPUT_LINESEP: CSV_LINESEP,
    GCS_MONGO_INPUT_LOCALE: CSV_LOCALE,
    GCS_MONGO_INPUT_MAXCHARSPERCOLUMN: CSV_MAXCHARSPERCOLUMN,
    GCS_MONGO_INPUT_MAXCOLUMNS: CSV_MAXCOLUMNS,
    GCS_MONGO_INPUT_MODE: CSV_MODE,
    GCS_MONGO_INPUT_MULTILINE: CSV_MULTILINE,
    GCS_MONGO_INPUT_NANVALUE: CSV_NANVALUE,
    GCS_MONGO_INPUT_NULLVALUE: CSV_NULLVALUE,
    GCS_MONGO_INPUT_NEGATIVEINF: CSV_NEGATIVEINF,
    GCS_MONGO_INPUT_POSITIVEINF: CSV_POSITIVEINF,
    GCS_MONGO_INPUT_QUOTE: CSV_QUOTE,
    GCS_MONGO_INPUT_SAMPLINGRATIO: CSV_SAMPLINGRATIO,
    GCS_MONGO_INPUT_SEP: CSV_SEP,
    GCS_MONGO_INPUT_TIMESTAMPFORMAT: CSV_TIMESTAMPFORMAT,
    GCS_MONGO_INPUT_TIMESTAMPNTZFORMAT: CSV_TIMESTAMPNTZFORMAT,
    GCS_MONGO_INPUT_UNESCAPEDQUOTEHANDLING: CSV_UNESCAPEDQUOTEHANDLING,
}

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
GCS_BT_INPUT_CHARTOESCAPEQUOTEESCAPING = "gcs.bigtable.input.chartoescapequoteescaping"
GCS_BT_INPUT_COLUMNNAMEOFCORRUPTRECORD = "gcs.bigtable.input.columnnameofcorruptrecord"
GCS_BT_INPUT_COMMENT = "gcs.bigtable.input.comment"
GCS_BT_INPUT_DATEFORMAT = "gcs.bigtable.input.dateformat"
GCS_BT_INPUT_EMPTYVALUE = "gcs.bigtable.input.emptyvalue"
GCS_BT_INPUT_ENCODING = "gcs.bigtable.input.encoding"
GCS_BT_INPUT_ENFORCESCHEMA = "gcs.bigtable.input.enforceschema"
GCS_BT_INPUT_ESCAPE = "gcs.bigtable.input.escape"
GCS_BT_INPUT_HEADER = "gcs.bigtable.input.header"
GCS_BT_INPUT_IGNORELEADINGWHITESPACE = "gcs.bigtable.input.ignoreleadingwhitespace"
GCS_BT_INPUT_IGNORETRAILINGWHITESPACE = "gcs.bigtable.input.ignoretrailingwhitespace"
GCS_BT_INPUT_INFER_SCHEMA = "gcs.bigtable.input.inferschema"
GCS_BT_INPUT_LINESEP = "gcs.bigtable.input.linesep"
GCS_BT_INPUT_LOCALE = "gcs.bigtable.input.locale"
GCS_BT_INPUT_MAXCHARSPERCOLUMN = "gcs.bigtable.input.maxcharspercolumn"
GCS_BT_INPUT_MAXCOLUMNS = "gcs.bigtable.input.maxcolumns"
GCS_BT_INPUT_MODE = "gcs.bigtable.input.mode"
GCS_BT_INPUT_MULTILINE = "gcs.bigtable.input.multiline"
GCS_BT_INPUT_NANVALUE = "gcs.bigtable.input.nanvalue"
GCS_BT_INPUT_NULLVALUE = "gcs.bigtable.input.nullvalue"
GCS_BT_INPUT_NEGATIVEINF = "gcs.bigtable.input.negativeinf"
GCS_BT_INPUT_POSITIVEINF = "gcs.bigtable.input.positiveinf"
GCS_BT_INPUT_QUOTE = "gcs.bigtable.input.quote"
GCS_BT_INPUT_SAMPLINGRATIO = "gcs.bigtable.input.samplingratio"
GCS_BT_INPUT_SEP = "gcs.bigtable.input.sep"
GCS_BT_INPUT_TIMESTAMPFORMAT = "gcs.bigtable.input.timestampformat"
GCS_BT_INPUT_TIMESTAMPNTZFORMAT = "gcs.bigtable.input.timestampntzformat"
GCS_BT_INPUT_UNESCAPEDQUOTEHANDLING = "gcs.bigtable.input.unescapedquotehandling"
# Template options linked to JDBC option names.
GCS_BT_INPUT_SPARK_OPTIONS = {
    GCS_BT_INPUT_CHARTOESCAPEQUOTEESCAPING: CSV_CHARTOESCAPEQUOTEESCAPING,
    GCS_BT_INPUT_COLUMNNAMEOFCORRUPTRECORD: CSV_COLUMNNAMEOFCORRUPTRECORD,
    GCS_BT_INPUT_COMMENT: CSV_COMMENT,
    GCS_BT_INPUT_DATEFORMAT: CSV_DATEFORMAT,
    GCS_BT_INPUT_EMPTYVALUE: CSV_EMPTYVALUE,
    GCS_BT_INPUT_ENCODING: CSV_ENCODING,
    GCS_BT_INPUT_ENFORCESCHEMA: CSV_ENFORCESCHEMA,
    GCS_BT_INPUT_ESCAPE: CSV_ESCAPE,
    GCS_BT_INPUT_HEADER: CSV_HEADER,
    GCS_BT_INPUT_IGNORELEADINGWHITESPACE: CSV_IGNORELEADINGWHITESPACE,
    GCS_BT_INPUT_IGNORETRAILINGWHITESPACE: CSV_IGNORETRAILINGWHITESPACE,
    GCS_BT_INPUT_INFER_SCHEMA: CSV_INFER_SCHEMA,
    GCS_BT_INPUT_LINESEP: CSV_LINESEP,
    GCS_BT_INPUT_LOCALE: CSV_LOCALE,
    GCS_BT_INPUT_MAXCHARSPERCOLUMN: CSV_MAXCHARSPERCOLUMN,
    GCS_BT_INPUT_MAXCOLUMNS: CSV_MAXCOLUMNS,
    GCS_BT_INPUT_MODE: CSV_MODE,
    GCS_BT_INPUT_MULTILINE: CSV_MULTILINE,
    GCS_BT_INPUT_NANVALUE: CSV_NANVALUE,
    GCS_BT_INPUT_NULLVALUE: CSV_NULLVALUE,
    GCS_BT_INPUT_NEGATIVEINF: CSV_NEGATIVEINF,
    GCS_BT_INPUT_POSITIVEINF: CSV_POSITIVEINF,
    GCS_BT_INPUT_QUOTE: CSV_QUOTE,
    GCS_BT_INPUT_SAMPLINGRATIO: CSV_SAMPLINGRATIO,
    GCS_BT_INPUT_SEP: CSV_SEP,
    GCS_BT_INPUT_TIMESTAMPFORMAT: CSV_TIMESTAMPFORMAT,
    GCS_BT_INPUT_TIMESTAMPNTZFORMAT: CSV_TIMESTAMPNTZFORMAT,
    GCS_BT_INPUT_UNESCAPEDQUOTEHANDLING: CSV_UNESCAPEDQUOTEHANDLING,
}

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
GCS_TO_GCS_INPUT_CHARTOESCAPEQUOTEESCAPING = "gcs.to.gcs.input.chartoescapequoteescaping"
GCS_TO_GCS_INPUT_COLUMNNAMEOFCORRUPTRECORD = "gcs.to.gcs.input.columnnameofcorruptrecord"
GCS_TO_GCS_INPUT_COMMENT = "gcs.to.gcs.input.comment"
GCS_TO_GCS_INPUT_DATEFORMAT = "gcs.to.gcs.input.dateformat"
GCS_TO_GCS_INPUT_EMPTYVALUE = "gcs.to.gcs.input.emptyvalue"
GCS_TO_GCS_INPUT_ENCODING = "gcs.to.gcs.input.encoding"
GCS_TO_GCS_INPUT_ENFORCESCHEMA = "gcs.to.gcs.input.enforceschema"
GCS_TO_GCS_INPUT_ESCAPE = "gcs.to.gcs.input.escape"
GCS_TO_GCS_INPUT_HEADER = "gcs.to.gcs.input.header"
GCS_TO_GCS_INPUT_IGNORELEADINGWHITESPACE = "gcs.to.gcs.input.ignoreleadingwhitespace"
GCS_TO_GCS_INPUT_IGNORETRAILINGWHITESPACE = "gcs.to.gcs.input.ignoretrailingwhitespace"
GCS_TO_GCS_INPUT_INFER_SCHEMA = "gcs.to.gcs.input.inferschema"
GCS_TO_GCS_INPUT_LINESEP = "gcs.to.gcs.input.linesep"
GCS_TO_GCS_INPUT_LOCALE = "gcs.to.gcs.input.locale"
GCS_TO_GCS_INPUT_MAXCHARSPERCOLUMN = "gcs.to.gcs.input.maxcharspercolumn"
GCS_TO_GCS_INPUT_MAXCOLUMNS = "gcs.to.gcs.input.maxcolumns"
GCS_TO_GCS_INPUT_MODE = "gcs.to.gcs.input.mode"
GCS_TO_GCS_INPUT_MULTILINE = "gcs.to.gcs.input.multiline"
GCS_TO_GCS_INPUT_NANVALUE = "gcs.to.gcs.input.nanvalue"
GCS_TO_GCS_INPUT_NULLVALUE = "gcs.to.gcs.input.nullvalue"
GCS_TO_GCS_INPUT_NEGATIVEINF = "gcs.to.gcs.input.negativeinf"
GCS_TO_GCS_INPUT_POSITIVEINF = "gcs.to.gcs.input.positiveinf"
GCS_TO_GCS_INPUT_QUOTE = "gcs.to.gcs.input.quote"
GCS_TO_GCS_INPUT_SAMPLINGRATIO = "gcs.to.gcs.input.samplingratio"
GCS_TO_GCS_INPUT_SEP = "gcs.to.gcs.input.sep"
GCS_TO_GCS_INPUT_TIMESTAMPFORMAT = "gcs.to.gcs.input.timestampformat"
GCS_TO_GCS_INPUT_TIMESTAMPNTZFORMAT = "gcs.to.gcs.input.timestampntzformat"
GCS_TO_GCS_INPUT_UNESCAPEDQUOTEHANDLING = "gcs.to.gcs.input.unescapedquotehandling"
# Template options linked to JDBC option names.
GCS_TO_GCS_INPUT_SPARK_OPTIONS = {
    GCS_TO_GCS_INPUT_CHARTOESCAPEQUOTEESCAPING: CSV_CHARTOESCAPEQUOTEESCAPING,
    GCS_TO_GCS_INPUT_COLUMNNAMEOFCORRUPTRECORD: CSV_COLUMNNAMEOFCORRUPTRECORD,
    GCS_TO_GCS_INPUT_COMMENT: CSV_COMMENT,
    GCS_TO_GCS_INPUT_DATEFORMAT: CSV_DATEFORMAT,
    GCS_TO_GCS_INPUT_EMPTYVALUE: CSV_EMPTYVALUE,
    GCS_TO_GCS_INPUT_ENCODING: CSV_ENCODING,
    GCS_TO_GCS_INPUT_ENFORCESCHEMA: CSV_ENFORCESCHEMA,
    GCS_TO_GCS_INPUT_ESCAPE: CSV_ESCAPE,
    GCS_TO_GCS_INPUT_HEADER: CSV_HEADER,
    GCS_TO_GCS_INPUT_IGNORELEADINGWHITESPACE: CSV_IGNORELEADINGWHITESPACE,
    GCS_TO_GCS_INPUT_IGNORETRAILINGWHITESPACE: CSV_IGNORETRAILINGWHITESPACE,
    GCS_TO_GCS_INPUT_INFER_SCHEMA: CSV_INFER_SCHEMA,
    GCS_TO_GCS_INPUT_LINESEP: CSV_LINESEP,
    GCS_TO_GCS_INPUT_LOCALE: CSV_LOCALE,
    GCS_TO_GCS_INPUT_MAXCHARSPERCOLUMN: CSV_MAXCHARSPERCOLUMN,
    GCS_TO_GCS_INPUT_MAXCOLUMNS: CSV_MAXCOLUMNS,
    GCS_TO_GCS_INPUT_MODE: CSV_MODE,
    GCS_TO_GCS_INPUT_MULTILINE: CSV_MULTILINE,
    GCS_TO_GCS_INPUT_NANVALUE: CSV_NANVALUE,
    GCS_TO_GCS_INPUT_NULLVALUE: CSV_NULLVALUE,
    GCS_TO_GCS_INPUT_NEGATIVEINF: CSV_NEGATIVEINF,
    GCS_TO_GCS_INPUT_POSITIVEINF: CSV_POSITIVEINF,
    GCS_TO_GCS_INPUT_QUOTE: CSV_QUOTE,
    GCS_TO_GCS_INPUT_SAMPLINGRATIO: CSV_SAMPLINGRATIO,
    GCS_TO_GCS_INPUT_SEP: CSV_SEP,
    GCS_TO_GCS_INPUT_TIMESTAMPFORMAT: CSV_TIMESTAMPFORMAT,
    GCS_TO_GCS_INPUT_TIMESTAMPNTZFORMAT: CSV_TIMESTAMPNTZFORMAT,
    GCS_TO_GCS_INPUT_UNESCAPEDQUOTEHANDLING: CSV_UNESCAPEDQUOTEHANDLING,
}

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
TEXT_BQ_INPUT_CHARTOESCAPEQUOTEESCAPING = "text.bigquery.input.chartoescapequoteescaping"
TEXT_BQ_INPUT_COLUMNNAMEOFCORRUPTRECORD = "text.bigquery.input.columnnameofcorruptrecord"
TEXT_BQ_INPUT_COMMENT = "text.bigquery.input.comment"
TEXT_BQ_INPUT_DATEFORMAT = "text.bigquery.input.dateformat"
TEXT_BQ_INPUT_EMPTYVALUE = "text.bigquery.input.emptyvalue"
TEXT_BQ_INPUT_ENCODING = "text.bigquery.input.encoding"
TEXT_BQ_INPUT_ENFORCESCHEMA = "text.bigquery.input.enforceschema"
TEXT_BQ_INPUT_ESCAPE = "text.bigquery.input.escape"
TEXT_BQ_INPUT_HEADER = "text.bigquery.input.header"
TEXT_BQ_INPUT_IGNORELEADINGWHITESPACE = "text.bigquery.input.ignoreleadingwhitespace"
TEXT_BQ_INPUT_IGNORETRAILINGWHITESPACE = "text.bigquery.input.ignoretrailingwhitespace"
TEXT_BQ_INPUT_INFER_SCHEMA = "text.bigquery.input.inferschema"
TEXT_BQ_INPUT_LINESEP = "text.bigquery.input.linesep"
TEXT_BQ_INPUT_LOCALE = "text.bigquery.input.locale"
TEXT_BQ_INPUT_MAXCHARSPERCOLUMN = "text.bigquery.input.maxcharspercolumn"
TEXT_BQ_INPUT_MAXCOLUMNS = "text.bigquery.input.maxcolumns"
TEXT_BQ_INPUT_MODE = "text.bigquery.input.mode"
TEXT_BQ_INPUT_MULTILINE = "text.bigquery.input.multiline"
TEXT_BQ_INPUT_NANVALUE = "text.bigquery.input.nanvalue"
TEXT_BQ_INPUT_NULLVALUE = "text.bigquery.input.nullvalue"
TEXT_BQ_INPUT_NEGATIVEINF = "text.bigquery.input.negativeinf"
TEXT_BQ_INPUT_POSITIVEINF = "text.bigquery.input.positiveinf"
TEXT_BQ_INPUT_QUOTE = "text.bigquery.input.quote"
TEXT_BQ_INPUT_SAMPLINGRATIO = "text.bigquery.input.samplingratio"
TEXT_BQ_INPUT_SEP = "text.bigquery.input.sep"
TEXT_BQ_INPUT_TIMESTAMPFORMAT = "text.bigquery.input.timestampformat"
TEXT_BQ_INPUT_TIMESTAMPNTZFORMAT = "text.bigquery.input.timestampntzformat"
TEXT_BQ_INPUT_UNESCAPEDQUOTEHANDLING = "text.bigquery.input.unescapedquotehandling"
# Optional CSV options linked to JDBC option names.
TEXT_BQ_INPUT_SPARK_OPTIONS = {
    TEXT_BQ_INPUT_CHARTOESCAPEQUOTEESCAPING: CSV_CHARTOESCAPEQUOTEESCAPING,
    TEXT_BQ_INPUT_COLUMNNAMEOFCORRUPTRECORD: CSV_COLUMNNAMEOFCORRUPTRECORD,
    TEXT_BQ_INPUT_COMMENT: CSV_COMMENT,
    TEXT_BQ_INPUT_DATEFORMAT: CSV_DATEFORMAT,
    TEXT_BQ_INPUT_EMPTYVALUE: CSV_EMPTYVALUE,
    TEXT_BQ_INPUT_ENCODING: CSV_ENCODING,
    TEXT_BQ_INPUT_ENFORCESCHEMA: CSV_ENFORCESCHEMA,
    TEXT_BQ_INPUT_ESCAPE: CSV_ESCAPE,
    TEXT_BQ_INPUT_HEADER: CSV_HEADER,
    TEXT_BQ_INPUT_IGNORELEADINGWHITESPACE: CSV_IGNORELEADINGWHITESPACE,
    TEXT_BQ_INPUT_IGNORETRAILINGWHITESPACE: CSV_IGNORETRAILINGWHITESPACE,
    TEXT_BQ_INPUT_INFER_SCHEMA: CSV_INFER_SCHEMA,
    TEXT_BQ_INPUT_LINESEP: CSV_LINESEP,
    TEXT_BQ_INPUT_LOCALE: CSV_LOCALE,
    TEXT_BQ_INPUT_MAXCHARSPERCOLUMN: CSV_MAXCHARSPERCOLUMN,
    TEXT_BQ_INPUT_MAXCOLUMNS: CSV_MAXCOLUMNS,
    TEXT_BQ_INPUT_MODE: CSV_MODE,
    TEXT_BQ_INPUT_MULTILINE: CSV_MULTILINE,
    TEXT_BQ_INPUT_NANVALUE: CSV_NANVALUE,
    TEXT_BQ_INPUT_NULLVALUE: CSV_NULLVALUE,
    TEXT_BQ_INPUT_NEGATIVEINF: CSV_NEGATIVEINF,
    TEXT_BQ_INPUT_POSITIVEINF: CSV_POSITIVEINF,
    TEXT_BQ_INPUT_QUOTE: CSV_QUOTE,
    TEXT_BQ_INPUT_SAMPLINGRATIO: CSV_SAMPLINGRATIO,
    TEXT_BQ_INPUT_SEP: CSV_SEP,
    TEXT_BQ_INPUT_TIMESTAMPFORMAT: CSV_TIMESTAMPFORMAT,
    TEXT_BQ_INPUT_TIMESTAMPNTZFORMAT: CSV_TIMESTAMPNTZFORMAT,
    TEXT_BQ_INPUT_UNESCAPEDQUOTEHANDLING: CSV_UNESCAPEDQUOTEHANDLING,
}

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
JDBCTOJDBC_SESSIONINITSTATEMENT = "jdbctojdbc.input.sessioninitstatement"
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
JDBCTOGCS_INPUT_SQL_QUERY = "jdbctogcs.input.sql.query"
JDBCTOGCS_INPUT_FETCHSIZE = "jdbctogcs.input.fetchsize"
JDBCTOGCS_INPUT_PARTITIONCOLUMN = "jdbctogcs.input.partitioncolumn"
JDBCTOGCS_INPUT_LOWERBOUND = "jdbctogcs.input.lowerbound"
JDBCTOGCS_INPUT_UPPERBOUND = "jdbctogcs.input.upperbound"
JDBCTOGCS_SESSIONINITSTATEMENT = "jdbctogcs.input.sessioninitstatement"
JDBCTOGCS_NUMPARTITIONS = "jdbctogcs.numpartitions"
JDBCTOGCS_OUTPUT_LOCATION = "jdbctogcs.output.location"
JDBCTOGCS_OUTPUT_FORMAT = "jdbctogcs.output.format"
JDBCTOGCS_OUTPUT_MODE = "jdbctogcs.output.mode"
JDBCTOGCS_OUTPUT_PARTITIONCOLUMN = "jdbctogcs.output.partitioncolumn"
JDBCTOGCS_TEMP_VIEW_NAME = "jdbctogcs.temp.view.name"
JDBCTOGCS_TEMP_SQL_QUERY = "jdbctogcs.temp.sql.query"

# JDBC to BigQuery
JDBC_BQ_INPUT_URL = "jdbc.bigquery.input.url"
JDBC_BQ_INPUT_DRIVER = "jdbc.bigquery.input.driver"
JDBC_BQ_INPUT_TABLE = "jdbc.bigquery.input.table"
JDBC_BQ_INPUT_FETCHSIZE = "jdbc.bigquery.input.fetchsize"
JDBC_BQ_INPUT_PARTITIONCOLUMN = "jdbc.bigquery.input.partitioncolumn"
JDBC_BQ_INPUT_LOWERBOUND = "jdbc.bigquery.input.lowerbound"
JDBC_BQ_INPUT_UPPERBOUND = "jdbc.bigquery.input.upperbound"
JDBC_BQ_SESSIONINITSTATEMENT = "jdbc.bigquery.input.sessioninitstatement"
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

# Cassandra To GCS
CASSANDRA_TO_GCS_INPUT_KEYSPACE = "cassandratogcs.input.keyspace"
CASSANDRA_TO_GCS_INPUT_TABLE = "cassandratogcs.input.table"
CASSANDRA_TO_GCS_INPUT_HOST = "cassandratogcs.input.host"
CASSANDRA_TO_GCS_OUTPUT_FORMAT = "cassandratogcs.output.format"
CASSANDRA_TO_GCS_OUTPUT_PATH = "cassandratogcs.output.path"
CASSANDRA_TO_GCS_OUTPUT_SAVEMODE = "cassandratogcs.output.savemode"
CASSANDRA_TO_GCS_CATALOG = "cassandratogcs.input.catalog.name"
CASSANDRA_TO_GCS_QUERY = "cassandratogcs.input.query"

# Hive DDL Extractor Util
HIVE_DDL_EXTRACTOR_INPUT_DATABASE = "hive.ddl.extractor.input.database"
HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH = "hive.ddl.extractor.output.path"
HIVE_DDL_CONSIDER_SPARK_TABLES = "hive.ddl.consider.spark.tables"
HIVE_DDL_TRANSLATION_DISPOSITION = "hive.ddl.translation.disposition"

# AWS S3 To BigQuery
S3_BQ_INPUT_LOCATION = "s3.bq.input.location"
S3_BQ_INPUT_FORMAT = "s3.bq.input.format"
S3_BQ_ACCESS_KEY = "s3.bq.access.key"
S3_BQ_SECRET_KEY = "s3.bq.secret.key"
S3_BQ_OUTPUT_DATASET_NAME = "s3.bq.output.dataset.name"
S3_BQ_OUTPUT_TABLE_NAME = "s3.bq.output.table.name"
S3_BQ_TEMP_BUCKET_NAME = "s3.bq.temp.bucket.name"
S3_BQ_OUTPUT_MODE = "s3.bq.output.mode"
S3_BQ_ENDPOINT_VALUE = "s3.amazonaws.com"


#Kafka To Bq
KAFKA_BQ_CHECKPOINT_LOCATION='kafka.to.bq.checkpoint.location'
KAFKA_BOOTSTRAP_SERVERS='kafka.to.bq.bootstrap.servers'
KAFKA_BQ_TOPIC='kafka.to.bq.topic'
KAFKA_BQ_STARTING_OFFSET='kafka.to.bq.starting.offset'
KAFKA_BQ_DATASET='kafka.to.bq.dataset'
KAFKA_BQ_TABLE_NAME='kafka.to.bq.table'
KAFKA_BQ_TEMP_BUCKET_NAME='kafka.to.bq.temp.bucket.name'
KAFKA_BQ_TERMINATION_TIMEOUT='kafka.to.bq.termination.timeout'
KAFKA_INPUT_FORMAT='kafka'
KAFKA_BQ_OUTPUT_MODE='kafka.to.bq.output.mode'

KAFKA_INPUT_FORMAT='kafka'

#Kafka To GCS
KAFKA_GCS_BOOTSTRAP_SERVERS='kafka.gcs.bootstrap.servers'
KAFKA_GCS_OUTPUT_LOCATION='kafka.gcs.output.location.gcs.path'
KAFKA_TOPIC='kafka.gcs.topic'
KAFKA_GCS_OUTPUT_FORMAT='kafka.gcs.output.format'
KAFKA_GCS_OUPUT_MODE='kafka.gcs.output.mode'
KAFKA_GCS_TERMINATION_TIMEOUT='kafka.gcs.termination.timeout'
KAFKA_STARTING_OFFSET='kafka.gcs.starting.offset'
KAFKA_GCS_CHECKPOINT_LOCATION='kafka.gcs.checkpoint.location'

#Pubsublite To GCS
PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL = "pubsublite.to.gcs.input.subscription.url"
PUBSUBLITE_TO_GCS_WRITE_MODE = "pubsublite.to.gcs.write.mode"
PUBSUBLITE_TO_GCS_OUTPUT_LOCATION = "pubsublite.to.gcs.output.location"
PUBSUBLITE_CHECKPOINT_LOCATION = "pubsublite.to.gcs.checkpoint.location"
PUBSUBLITE_TO_GCS_OUTPUT_FORMAT = "pubsublite.to.gcs.output.format"
PUBSUBLITE_TO_GCS_TIMEOUT = "pubsublite.to.gcs.timeout"
PUBSUBLITE_TO_GCS_PROCESSING_TIME = "pubsublite.to.gcs.processing.time"

# Azure Storage to BigQuery
AZ_BLOB_BQ_INPUT_LOCATION = "azure.blob.bigquery.input.location"
AZ_BLOB_BQ_INPUT_FORMAT = "azure.blob.bigquery.input.format"
AZ_BLOB_BQ_OUTPUT_DATASET = "azure.blob.bigquery.output.dataset"
AZ_BLOB_BQ_OUTPUT_TABLE = "azure.blob.bigquery.output.table"
AZ_BLOB_BQ_OUTPUT_MODE = "azure.blob.bigquery.output.mode"
AZ_BLOB_BQ_TEMP_BUCKET = "temporaryGcsBucket"
AZ_BLOB_BQ_LD_TEMP_BUCKET_NAME = "azure.blob.bigquery.temp.bucket.name"
AZ_BLOB_STORAGE_ACCOUNT = "azure.blob.storage.account"
AZ_BLOB_CONTAINER_NAME = "azure.blob.container.name"
AZ_BLOB_SAS_TOKEN = "azure.blob.sas.token"
