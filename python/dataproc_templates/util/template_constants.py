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
FORMAT_BIGQUERY = "bigquery"
FORMAT_JDBC = "jdbc"
FORMAT_PUBSUBLITE = "pubsublite"
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
CSV_COMPRESSION = "compression"
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
FORMAT_BIGTABLE = "bigtable"
TABLE = "table"
TEMP_GCS_BUCKET = "temporaryGcsBucket"
WRITE_METHOD="writeMethod"
MONGO_URL = "spark.mongodb.output.uri"
MONGO_INPUT_URI = "spark.mongodb.input.uri"
MONGO_DATABASE = "database"
MONGO_COLLECTION = "collection"
FORMAT_MONGO = "com.mongodb.spark.sql.DefaultSource"
MONGO_DEFAULT_BATCH_SIZE = 512
MONGO_BATCH_SIZE = "maxBatchSize"
FORMAT_SNOWFLAKE = "snowflake"
REDSHIFT_TEMPDIR = "tempdir"
REDSHIFT_IAMROLE = "aws_iam_role"
AWS_S3ACCESSKEY = "fs.s3a.access.key"
AWS_S3SECRETKEY = "fs.s3a.secret.key"
AWS_S3ENDPOINT = "fs.s3a.endpoint"
SQL_EXTENSION = "spark.sql.extensions"
CASSANDRA_EXTENSION = "com.datastax.spark.connector.CassandraSparkExtensions"
CASSANDRA_CATALOG = "com.datastax.spark.connector.datasource.CassandraCatalog"
FORMAT_PUBSUBLITE = "pubsublite"
PUBSUBLITE_SUBSCRIPTION = "pubsublite.subscription"
PUBSUBLITE_CHECKPOINT_LOCATION = "checkpointLocation"
STREAM_PATH = "path"
STREAM_CHECKPOINT_LOCATION = "checkpointLocation"
FORMAT_ELASTICSEARCH="org.elasticsearch.hadoop.mr.EsInputFormat"
ELASTICSEARCH_KEY_CLASS="org.apache.hadoop.io.NullWritable"
ELASTICSEARCH_VALUE_CLASS="org.elasticsearch.hadoop.mr.LinkedMapWritable"
ES_NODES_PATH_PREFIX="es.nodes.path.prefix"
ES_QUERY="es.query"
ES_MAPPING_DATE_RICH="es.mapping.date.rich"
ES_READ_FIELD_INCLUDE="es.read.field.include"
ES_READ_FIELD_EXCLUDE="es.read.field.exclude"
ES_READ_FIELD_AS_ARRAY_INCLUDE="es.read.field.as.array.include"
ES_READ_FIELD_AS_ARRAY_EXCLUDE="es.read.field.as.array.exclude"
ES_READ_METADATA="es.read.metadata"
ES_READ_METADATA_FIELD="es.read.metadata.field"
ES_READ_METADATA_VERSION="es.read.metadata.version"
ES_INDEX_READ_MISSING_AS_EMPTY="es.index.read.missing.as.empty"
ES_FIELD_READ_EMPTY_AS_NULL="es.field.read.empty.as.null"
ES_READ_SHARD_PREFERENCE="es.read.shard.preference"
ES_READ_SOURCE_FILTER="es.read.source.filter"
ES_INDEX_READ_ALLOW_RED_STATUS="es.index.read.allow.red.status"
ES_INPUT_MAX_DOC_PER_PARTITION="es.input.max.docs.per.partition"
ES_NODES_DISCOVERY="es.nodes.discovery"
ES_NODES_CLIENT_ONLY="es.nodes.client.only"
ES_NODES_DATA_ONLY="es.nodes.data.only"
ES_NODES_WAN_ONLY="es.nodes.wan.only"
ES_HTTP_TIMEOUT="es.http.timeout"
ES_HTTP_RETRIES="es.http.retries"
ES_SCROLL_KEEPALIVE="es.scroll.keepalive"
ES_SCROLL_SIZE="es.scroll.size"
ES_SCROLL_LIMIT="es.scroll.limit"
ES_ACTION_HEART_BEAT_LEAD="es.action.heart.beat.lead"
ES_NET_HTTP_HEADER_AUTHORIZATION="es.net.http.header.Authorization"
ES_NET_SSL="es.net.ssl"
ES_NET_SSL_CERT_ALLOW_SELF_SIGNED="es.net.ssl.cert.allow.self.signed"
ES_NET_SSL_PROTOCOL="es.net.ssl.protocol"
ES_NET_PROXY_HTTP_HOST="es.net.proxy.http.host"
ES_NET_PROXY_HTTP_PORT="es.net.proxy.http.port"
ES_NET_PROXY_HTTP_USER="es.net.proxy.http.user"
ES_NET_PROXY_HTTP_PASS="es.net.proxy.http.pass"
ES_NET_PROXY_HTTP_USE_SYSTEM_PROPS="es.net.proxy.http.use.system.props"
ES_NET_PROXY_HTTPS_HOST="es.net.proxy.https.host"
ES_NET_PROXY_HTTPS_PORT="es.net.proxy.https.port"
ES_NET_PROXY_HTTPS_USER="es.net.proxy.https.user"
ES_NET_PROXY_HTTPS_PASS="es.net.proxy.https.pass"
ES_NET_PROXY_HTTPS_USE_SYSTEM_PROPS="es.net.proxy.https.use.system.props"
ES_NET_PROXY_SOCKS_HOST="es.net.proxy.socks.host"
ES_NET_PROXY_SOCKS_PORT="es.net.proxy.socks.port"
ES_NET_PROXY_SOCKS_USER="es.net.proxy.socks.user"
ES_NET_PROXY_SOCKS_PASS="es.net.proxy.socks.pass"
ES_NET_PROXY_SOCKS_USE_SYSTEM_PROPS="es.net.proxy.socks.use.system.props"

OPTION_DEFAULT = "default"
OPTION_HELP = "help"
OPTION_READ_HELP = "read_help"
OPTION_WRITE_HELP = "write_help"

# At the moment this is just a map of CSV related options but it will be expanded as required for other uses.
SPARK_OPTIONS = {
    CSV_CHARTOESCAPEQUOTEESCAPING:
        {OPTION_HELP: "Sets a single character used for escaping the escape for the quote character. "
                      "The default value is escape character when escape and quote characters are "
                      "different, \\0 otherwise"},
    CSV_COLUMNNAMEOFCORRUPTRECORD:
        {OPTION_READ_HELP: "Allows renaming the new field having malformed "
                           "string created by PERMISSIVE mode"},
    CSV_COMMENT:
        {OPTION_READ_HELP: "Sets a single character used for skipping lines beginning with this "
                           "character. By default it is disabled"},
    CSV_COMPRESSION:
        {OPTION_WRITE_HELP: "Compression codec to use when saving to file. This can be one of the known "
                            "case-insensitive short names (none, bzip2, gzip, lz4, snappy and deflate)"},
    CSV_DATEFORMAT:
        {OPTION_HELP: "Sets the string that indicates a date format. This applies to date type"},
    CSV_EMPTYVALUE:
        {OPTION_HELP: "Sets the string representation of an empty value"},
    CSV_ENCODING:
        {OPTION_READ_HELP: "Decodes the CSV files by the given encoding type",
         OPTION_WRITE_HELP: "Specifies encoding (charset) of saved CSV files"},
    CSV_ENFORCESCHEMA:
        {OPTION_READ_HELP: "If it is set to true, the specified or inferred schema will be "
                           "forcibly applied to datasource files, and headers in CSV files "
                           "will be ignored. If the option is set to false, the schema will "
                           "be validated against all headers in CSV files in the case when "
                           "the header option is set to true. Defaults to True"},
    CSV_ESCAPE:
        {OPTION_HELP: "Sets a single character used for escaping quotes inside an already quoted value"},
    CSV_ESCAPEQUOTES:
        {OPTION_HELP: "A flag indicating whether values containing quotes should always be enclosed "
                      "in quotes. Default is to escape all values containing a quote character"},
    CSV_HEADER:
        {OPTION_DEFAULT: "true",
         OPTION_READ_HELP: "Uses the first line of CSV file as names of columns. Defaults to True",
         OPTION_WRITE_HELP: "Writes the names of columns as the first line. Defaults to True"},
    CSV_IGNORELEADINGWHITESPACE:
        {OPTION_HELP: "A flag indicating whether or not leading whitespaces from "
                      "values being read/written should be skipped"},
    CSV_IGNORETRAILINGWHITESPACE:
        {OPTION_HELP: "A flag indicating whether or not trailing whitespaces from "
                      "values being read/written should be skipped"},
    CSV_INFER_SCHEMA:
        {OPTION_DEFAULT: "true",
         OPTION_READ_HELP: "Infers the input schema automatically from data. It requires one "
                           "extra pass over the data. Defaults to True"},
    CSV_LINESEP:
        {OPTION_HELP: "Defines the line separator that should be used for parsing. "
                      "Defaults to \\r, \\r\\n and \\n for reading and \\n for writing"},
    CSV_LOCALE:
        {OPTION_READ_HELP: "Sets a locale as language tag in IETF BCP 47 format"},
    CSV_MAXCHARSPERCOLUMN:
        {OPTION_READ_HELP: "Defines the maximum number of characters allowed for any "
                           "given value being read. By default, it is -1 meaning unlimited length"},
    CSV_MAXCOLUMNS:
        {OPTION_READ_HELP: "Defines a hard limit of how many columns a record can have"},
    CSV_MODE:
        {OPTION_READ_HELP: "Allows a mode for dealing with corrupt records during parsing. It supports "
                           "the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST"},
    CSV_MULTILINE:
        {OPTION_READ_HELP: "Parse one record, which may span multiple lines, per file"},
    CSV_NANVALUE:
        {OPTION_READ_HELP: "Sets the string representation of a non-number value"},
    CSV_NULLVALUE:
        {OPTION_HELP: "Sets the string representation of a null value"},
    CSV_NEGATIVEINF:
        {OPTION_READ_HELP: "Sets the string representation of a negative infinity value"},
    CSV_POSITIVEINF:
        {OPTION_READ_HELP: "Sets the string representation of a positive infinity value"},
    CSV_QUOTE:
        {OPTION_READ_HELP: "Sets a single character used for escaping quoted values where the separator can "
                           "be part of the value. For reading, if you would like to turn off quotations, "
                           "you need to set not null but an empty string",
         OPTION_WRITE_HELP: "Sets a single character used for escaping quoted values where the separator can "
                            "be part of the value. For writing, if an empty string is set, it uses u0000 "
                            "(null character)"},
    CSV_QUOTEALL:
        {OPTION_WRITE_HELP: "A flag indicating whether all values should always be enclosed in quotes. "
                            "Default is to only escape values containing a quote character"},
    CSV_SAMPLINGRATIO:
        {OPTION_READ_HELP: "Defines fraction of rows used for schema inferring"},
    CSV_SEP:
        {OPTION_HELP: "Sets a separator for each field and value. This separator can be one or more characters"},
    CSV_TIMESTAMPFORMAT:
        {OPTION_HELP: "Sets the string that indicates a timestamp with timezone format"},
    CSV_TIMESTAMPNTZFORMAT:
        {OPTION_HELP: "Sets the string that indicates a timestamp without timezone format"},
    CSV_UNESCAPEDQUOTEHANDLING:
        {OPTION_READ_HELP: "Defines how the CsvParser will handle values with unescaped quotes."
                           "Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE, RAISE_ERROR"},
}

# Helper functions for applying SPARK_OPTIONS to templates


def get_csv_input_spark_options(prefix):
    input_options = [
        CSV_CHARTOESCAPEQUOTEESCAPING,
        CSV_COLUMNNAMEOFCORRUPTRECORD,
        CSV_COMMENT,
        CSV_DATEFORMAT,
        CSV_EMPTYVALUE,
        CSV_ENCODING,
        CSV_ENFORCESCHEMA,
        CSV_ESCAPE,
        CSV_HEADER,
        CSV_IGNORELEADINGWHITESPACE,
        CSV_IGNORETRAILINGWHITESPACE,
        CSV_INFER_SCHEMA,
        CSV_LINESEP,
        CSV_LOCALE,
        CSV_MAXCHARSPERCOLUMN,
        CSV_MAXCOLUMNS,
        CSV_MODE,
        CSV_MULTILINE,
        CSV_NANVALUE,
        CSV_NULLVALUE,
        CSV_NEGATIVEINF,
        CSV_POSITIVEINF,
        CSV_QUOTE,
        CSV_SAMPLINGRATIO,
        CSV_SEP,
        CSV_TIMESTAMPFORMAT,
        CSV_TIMESTAMPNTZFORMAT,
        CSV_UNESCAPEDQUOTEHANDLING,
    ]
    spark_options = {(prefix + _).lower(): _ for _ in input_options}
    return spark_options


def get_csv_output_spark_options(prefix):
    output_options = {
        CSV_CHARTOESCAPEQUOTEESCAPING,
        CSV_COMPRESSION,
        CSV_DATEFORMAT,
        CSV_EMPTYVALUE,
        CSV_ENCODING,
        CSV_ESCAPE,
        CSV_ESCAPEQUOTES,
        CSV_HEADER,
        CSV_IGNORELEADINGWHITESPACE,
        CSV_IGNORETRAILINGWHITESPACE,
        CSV_LINESEP,
        CSV_NULLVALUE,
        CSV_QUOTE,
        CSV_QUOTEALL,
        CSV_SEP,
        CSV_TIMESTAMPFORMAT,
        CSV_TIMESTAMPNTZFORMAT,
    }
    spark_options = {(prefix + _).lower(): _ for _ in output_options}
    return spark_options

# A map of Elasticsearch Spark Connector related options.
ES_SPARK_READER_OPTIONS = {
    ES_NODES_PATH_PREFIX:
        {OPTION_HELP: "Prefix to add to all requests made to Elasticsearch"},
    ES_QUERY:
        {OPTION_HELP: "Holds the query used for reading data from the specified Index"},
    ES_MAPPING_DATE_RICH:
        {OPTION_DEFAULT: "true",
         OPTION_HELP: "Whether to create a rich Date like object for Date fields in Elasticsearch "
                      "or returned them as primitives (String or long)"},
    ES_READ_FIELD_INCLUDE:
        {OPTION_HELP: "Fields/properties that are parsed and considered when reading the "
                      "documents from Elasticsearch. By default empty meaning all fields are considered"},
    ES_READ_FIELD_EXCLUDE:
        {OPTION_HELP: "Fields/properties that are discarded when reading the documents from Elasticsearch."
                      " By default empty meaning no fields are excluded"},
    ES_READ_FIELD_AS_ARRAY_INCLUDE:
        {OPTION_HELP: "Fields/properties that should be considered as arrays/lists"},
    ES_READ_FIELD_AS_ARRAY_EXCLUDE:
        {OPTION_HELP: "Fields/properties that should not be considered as arrays/lists"},
    ES_READ_METADATA:
        {OPTION_DEFAULT: "false",
         OPTION_HELP: "Whether to include the document metadata (such as id and version)"
                      " in the results or not"},
    ES_READ_METADATA_FIELD:
        {OPTION_DEFAULT: "_metadata",
         OPTION_HELP: "The field under which the metadata information is placed"},
    ES_READ_METADATA_VERSION:
        {OPTION_DEFAULT: "false",
         OPTION_HELP: "Whether to include the document version in the returned metadata"},
    ES_INDEX_READ_MISSING_AS_EMPTY:
        {OPTION_DEFAULT: "no",
         OPTION_HELP: "Whether elasticsearch-hadoop will allow reading of non existing indices"},
    ES_FIELD_READ_EMPTY_AS_NULL:
        {OPTION_DEFAULT: "yes",
         OPTION_HELP: "Whether elasticsearch-hadoop will treat empty fields as null"},
    ES_READ_SHARD_PREFERENCE:
        {OPTION_HELP: "The value to use for the shard preference of a search operation when executing "
                      "a scroll query"},
    ES_READ_SOURCE_FILTER:
        {OPTION_HELP: "Comma delimited string of field names that you would like to return from Elasticsearch"},
    ES_INDEX_READ_ALLOW_RED_STATUS:
        {OPTION_DEFAULT: "false",
         OPTION_HELP: "Fetch the data from the available shards and ignore the shards which are not reachable"},
    ES_INPUT_MAX_DOC_PER_PARTITION:
        {OPTION_HELP: "The maximum number of documents per input partition."
                      " This property is a suggestion, not a guarantee"},
    ES_NODES_DISCOVERY:
        {OPTION_DEFAULT: "true",
         OPTION_HELP: "Whether to discover the nodes within the Elasticsearch cluster or "
                      "only to use the ones given in es.nodes for metadata queries"},
    ES_NODES_CLIENT_ONLY:
        {OPTION_DEFAULT: "false",
         OPTION_HELP: "Whether to use Elasticsearch client nodes (or load-balancers)"},
    ES_NODES_DATA_ONLY:
        {OPTION_DEFAULT: "true",
         OPTION_HELP: "Whether to use Elasticsearch data nodes only"},
    ES_NODES_WAN_ONLY:
        {OPTION_DEFAULT: "false",
         OPTION_HELP: "Whether the connector is used against an Elasticsearch instance in "
                      "a cloud/restricted environment over the WAN, such as Amazon Web Services, "
                      "in order to use this option set es.gcs.input.es.nodes.discovery and "
                      "es.gcs.input.es.nodes.data.only to false"},
    ES_HTTP_TIMEOUT:
        {OPTION_DEFAULT: "1m",
         OPTION_HELP: "Timeout for HTTP/REST connections to Elasticsearch"},
    ES_HTTP_RETRIES:
        {OPTION_DEFAULT: "3",
         OPTION_HELP: "Number of retries for establishing a (broken) http connection"},
    ES_SCROLL_KEEPALIVE:
        {OPTION_DEFAULT: "10m",
         OPTION_HELP: "The maximum duration of result scrolls between query requests"},
    ES_SCROLL_SIZE:
        {OPTION_DEFAULT: "1000",
         OPTION_HELP: "Number of results/items/documents returned per scroll request on each executor/worker/task"},
    ES_SCROLL_LIMIT:
        {OPTION_DEFAULT: "-1",
         OPTION_HELP: "Number of total results/items returned by each individual scroll."
                      " A negative value indicates that all documents that match should be returned"},
    ES_ACTION_HEART_BEAT_LEAD:
        {OPTION_DEFAULT: "15s",
         OPTION_HELP: "The lead to task timeout before elasticsearch-hadoop informs Hadoop "
                      "the task is still running to prevent task restart"},
    ES_NET_HTTP_HEADER_AUTHORIZATION:
        {OPTION_HELP: "API Key for Elasticsearch Authorization"},
    ES_NET_SSL:
        {OPTION_DEFAULT: "false",
         OPTION_HELP: "Enable SSL"},
    ES_NET_SSL_CERT_ALLOW_SELF_SIGNED:
        {OPTION_DEFAULT: "false",
         OPTION_HELP: "Whether or not to allow self signed certificates"},
    ES_NET_SSL_PROTOCOL:
        {OPTION_DEFAULT: "TLS",
         OPTION_HELP: "SSL protocol to be used"},
    ES_NET_PROXY_HTTP_HOST:
        {OPTION_HELP: "Http proxy host name"},
    ES_NET_PROXY_HTTP_PORT:
        {OPTION_HELP: "Http proxy port"},
    ES_NET_PROXY_HTTP_USER:
        {OPTION_HELP: "Http proxy user name"},
    ES_NET_PROXY_HTTP_PASS:
        {OPTION_HELP: "Http proxy password"},
    ES_NET_PROXY_HTTP_USE_SYSTEM_PROPS:
        {OPTION_DEFAULT: "yes",
         OPTION_HELP: "Whether use the system Http proxy properties "
                      "(namely http.proxyHost and http.proxyPort) or not"},
    ES_NET_PROXY_HTTPS_HOST:
        {OPTION_HELP: "Https proxy host name"},
    ES_NET_PROXY_HTTPS_PORT:
        {OPTION_HELP: "Https proxy port"},
    ES_NET_PROXY_HTTPS_USER:
        {OPTION_HELP: "Https proxy user name"},
    ES_NET_PROXY_HTTPS_PASS:
        {OPTION_HELP: "Https proxy password"},
    ES_NET_PROXY_HTTPS_USE_SYSTEM_PROPS:
        {OPTION_DEFAULT: "yes",
         OPTION_HELP: "Whether use the system Https proxy properties "
                      "(namely https.proxyHost and https.proxyPort) or not"},
    ES_NET_PROXY_SOCKS_HOST:
        {OPTION_HELP: "Http proxy host name"},
    ES_NET_PROXY_SOCKS_PORT:
        {OPTION_HELP: "Http proxy port"},
    ES_NET_PROXY_SOCKS_USER:
        {OPTION_HELP: "Http proxy user name"},
    ES_NET_PROXY_SOCKS_PASS:
        {OPTION_HELP: "Http proxy password"},
    ES_NET_PROXY_SOCKS_USE_SYSTEM_PROPS:
        {OPTION_DEFAULT: "yes",
         OPTION_HELP: "Whether use the system Socks proxy properties "
                      "(namely socksProxyHost and socksProxyHost) or not"}
}

def get_es_spark_connector_input_options(prefix):
    input_options = [
        ES_NODES_PATH_PREFIX,
        ES_QUERY,
        ES_MAPPING_DATE_RICH,
        ES_READ_FIELD_INCLUDE,
        ES_READ_FIELD_EXCLUDE,
        ES_READ_FIELD_AS_ARRAY_INCLUDE,
        ES_READ_FIELD_AS_ARRAY_EXCLUDE,
        ES_READ_METADATA,
        ES_READ_METADATA_FIELD,
        ES_READ_METADATA_VERSION,
        ES_INDEX_READ_MISSING_AS_EMPTY,
        ES_FIELD_READ_EMPTY_AS_NULL,
        ES_READ_SHARD_PREFERENCE,
        ES_READ_SOURCE_FILTER,
        ES_INDEX_READ_ALLOW_RED_STATUS,
        ES_INPUT_MAX_DOC_PER_PARTITION,
        ES_NODES_DISCOVERY,
        ES_NODES_CLIENT_ONLY,
        ES_NODES_DATA_ONLY,
        ES_NODES_WAN_ONLY,
        ES_HTTP_TIMEOUT,
        ES_HTTP_RETRIES,
        ES_SCROLL_KEEPALIVE,
        ES_SCROLL_SIZE,
        ES_SCROLL_LIMIT,
        ES_ACTION_HEART_BEAT_LEAD,
        ES_NET_HTTP_HEADER_AUTHORIZATION,
        ES_NET_SSL,
        ES_NET_SSL_CERT_ALLOW_SELF_SIGNED,
        ES_NET_SSL_PROTOCOL,
        ES_NET_PROXY_HTTP_HOST,
        ES_NET_PROXY_HTTP_PORT,
        ES_NET_PROXY_HTTP_USER,
        ES_NET_PROXY_HTTP_PASS,
        ES_NET_PROXY_HTTP_USE_SYSTEM_PROPS,
        ES_NET_PROXY_HTTPS_HOST,
        ES_NET_PROXY_HTTPS_PORT,
        ES_NET_PROXY_HTTPS_USER,
        ES_NET_PROXY_HTTPS_PASS,
        ES_NET_PROXY_HTTPS_USE_SYSTEM_PROPS,
        ES_NET_PROXY_SOCKS_HOST,
        ES_NET_PROXY_SOCKS_PORT,
        ES_NET_PROXY_SOCKS_USER,
        ES_NET_PROXY_SOCKS_PASS,
        ES_NET_PROXY_SOCKS_USE_SYSTEM_PROPS,
    ]
    es_spark_connector_options = {(prefix + _).lower(): _ for _ in input_options}
    return es_spark_connector_options

# Output mode
OUTPUT_MODE_OVERWRITE = "overwrite"
OUTPUT_MODE_APPEND = "append"
OUTPUT_MODE_IGNORE = "ignore"
OUTPUT_MODE_ERRORIFEXISTS = "errorifexists"
OUTPUT_MODE_COMPLETE = "complete"
OUTPUT_MODE_UPDATE = "update"

#ES to GCS
ES_GCS_INPUT_NODE = "es.gcs.input.node"
ES_GCS_INPUT_INDEX = "es.gcs.input.index"
ES_GCS_NODE_USER = "es.gcs.input.user"
ES_GCS_NODE_PASSWORD = "es.gcs.input.password"
ES_GCS_OUTPUT_FORMAT = "es.gcs.output.format"
ES_GCS_OUTPUT_LOCATION = "es.gcs.output.location"
ES_GCS_OUTPUT_MODE = "es.gcs.output.mode"
ES_GCS_FLATTEN_STRUCT = "es.gcs.flatten.struct.fields"
ES_GCS_FLATTEN_ARRAY = "es.gcs.flatten.array.fields"

#ES to BQ
ES_BQ_INPUT_NODE = "es.bq.input.node"
ES_BQ_INPUT_INDEX = "es.bq.input.index"
ES_BQ_NODE_USER = "es.bq.input.user"
ES_BQ_NODE_PASSWORD = "es.bq.input.password"
ES_BQ_OUTPUT_DATASET = "es.bq.output.dataset"
ES_BQ_OUTPUT_TABLE = "es.bq.output.table"
ES_BQ_TEMP_BUCKET = "temporaryGcsBucket"
ES_BQ_LD_TEMP_BUCKET_NAME= "es.bq.temp.bucket.name"
ES_BQ_OUTPUT_MODE = "es.bq.output.mode"
ES_BQ_FLATTEN_STRUCT = "es.bq.flatten.struct.fields"
ES_BQ_FLATTEN_ARRAY = "es.bq.flatten.array.fields"

#ES to BigTable
ES_BT_INPUT_NODE = "es.bt.input.node"
ES_BT_INPUT_INDEX = "es.bt.input.index"
ES_BT_NODE_USER = "es.bt.input.user"
ES_BT_NODE_PASSWORD = "es.bt.input.password"
ES_BT_FLATTEN_STRUCT = "es.bt.flatten.struct.fields"
ES_BT_FLATTEN_ARRAY = "es.bt.flatten.array.fields"
ES_BT_CATALOG_JSON = "es.bt.catalog.json"
ES_BT_PROJECT_ID = "spark.bigtable.project.id"
ES_BT_INSTANCE_ID = "spark.bigtable.instance.id"
ES_BT_CREATE_NEW_TABLE = "spark.bigtable.create.new.table"
ES_BT_BATCH_MUTATE_SIZE = "spark.bigtable.batch.mutate.size"

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
GCS_JDBC_NUMPARTITIONS = "gcs.jdbc.numpartitions"

# GCS to Mongo
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

# Mongo to BQ
MONGO_BQ_INPUT_URI = "mongo.bq.input.uri"
MONGO_BQ_INPUT_DATABASE = "mongo.bq.input.database"
MONGO_BQ_INPUT_COLLECTION = "mongo.bq.input.collection"
MONGO_BQ_OUTPUT_DATASET = "mongo.bq.output.dataset"
MONGO_BQ_OUTPUT_TABLE = "mongo.bq.output.table"
MONGO_BQ_OUTPUT_MODE = "mongo.bq.output.mode"
MONGO_BQ_TEMP_BUCKET_NAME = "mongo.bq.temp.bucket.name"

# Cassandra to BQ
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
GCS_BT_CATALOG_JSON = "gcs.bigtable.catalog.json"
GCS_BT_CREATE_NEW_TABLE = "spark.bigtable.create.new.table"
GCS_BT_BATCH_MUTATE_SIZE = "spark.bigtable.batch.mutate.size"
GCS_BT_PROJECT_ID = "spark.bigtable.project.id"
GCS_BT_INSTANCE_ID = "spark.bigtable.instance.id"

# BigQuery to GCS
BQ_GCS_INPUT_TABLE = "bigquery.gcs.input.table"
BQ_GCS_OUTPUT_FORMAT = "bigquery.gcs.output.format"
BQ_GCS_OUTPUT_MODE = "bigquery.gcs.output.mode"
BQ_GCS_OUTPUT_PARTITION_COLUMN = "bigquery.gcs.output.partition.column"
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
HIVE_GCS_INPUT_DATABASE = "hive.gcs.input.database"
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

# Hbase to GCS
HBASE_GCS_OUTPUT_LOCATION = "hbase.gcs.output.location"
HBASE_GCS_OUTPUT_FORMAT = "hbase.gcs.output.format"
HBASE_GCS_OUTPUT_MODE = "hbase.gcs.output.mode"
HBASE_GCS_CATALOG_JSON = "hbase.gcs.catalog.json"

# JDBC to JDBC
JDBCTOJDBC_INPUT_URL = "jdbctojdbc.input.url"
JDBCTOJDBC_INPUT_URL_SECRET = "jdbctojdbc.input.url.secret"
JDBCTOJDBC_INPUT_DRIVER = "jdbctojdbc.input.driver"
JDBCTOJDBC_INPUT_TABLE = "jdbctojdbc.input.table"
JDBCTOJDBC_INPUT_FETCHSIZE = "jdbctojdbc.input.fetchsize"
JDBCTOJDBC_INPUT_PARTITIONCOLUMN = "jdbctojdbc.input.partitioncolumn"
JDBCTOJDBC_INPUT_LOWERBOUND = "jdbctojdbc.input.lowerbound"
JDBCTOJDBC_INPUT_UPPERBOUND = "jdbctojdbc.input.upperbound"
JDBCTOJDBC_SESSIONINITSTATEMENT = "jdbctojdbc.input.sessioninitstatement"
JDBCTOJDBC_NUMPARTITIONS = "jdbctojdbc.numpartitions"
JDBCTOJDBC_OUTPUT_URL = "jdbctojdbc.output.url"
JDBCTOJDBC_OUTPUT_URL_SECRET = "jdbctojdbc.output.url.secret"
JDBCTOJDBC_OUTPUT_DRIVER = "jdbctojdbc.output.driver"
JDBCTOJDBC_OUTPUT_TABLE = "jdbctojdbc.output.table"
JDBCTOJDBC_OUTPUT_CREATE_TABLE_OPTION = "jdbctojdbc.output.create_table.option"
JDBCTOJDBC_OUTPUT_MODE = "jdbctojdbc.output.mode"
JDBCTOJDBC_OUTPUT_BATCH_SIZE = "jdbctojdbc.output.batch.size"
JDBCTOJDBC_TEMP_VIEW_NAME = "jdbctojdbc.temp.view.name"
JDBCTOJDBC_SQL_QUERY = "jdbctojdbc.sql.query"

# JDBC to GCS
JDBCTOGCS_INPUT_URL = "jdbctogcs.input.url"
JDBCTOGCS_INPUT_URL_SECRET = "jdbctogcs.input.url.secret"
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
JDBC_BQ_INPUT_URL_SECRET = "jdbc.bigquery.input.url.secret"
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

# REDSHIFT to GCS
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


# Kafka To Bq
KAFKA_BQ_CHECKPOINT_LOCATION = 'kafka.to.bq.checkpoint.location'
KAFKA_BOOTSTRAP_SERVERS = 'kafka.to.bq.bootstrap.servers'
KAFKA_BQ_TOPIC = 'kafka.to.bq.topic'
KAFKA_BQ_STARTING_OFFSET = 'kafka.to.bq.starting.offset'
KAFKA_BQ_DATASET = 'kafka.to.bq.dataset'
KAFKA_BQ_TABLE_NAME = 'kafka.to.bq.table'
KAFKA_BQ_TEMP_BUCKET_NAME = 'kafka.to.bq.temp.bucket.name'
KAFKA_BQ_TERMINATION_TIMEOUT = 'kafka.to.bq.termination.timeout'
KAFKA_INPUT_FORMAT = 'kafka'
KAFKA_BQ_OUTPUT_MODE = 'kafka.to.bq.output.mode'

KAFKA_INPUT_FORMAT = 'kafka'

# Kafka To GCS
KAFKA_GCS_BOOTSTRAP_SERVERS = 'kafka.gcs.bootstrap.servers'
KAFKA_GCS_OUTPUT_LOCATION = 'kafka.gcs.output.location.gcs.path'
KAFKA_TOPIC = 'kafka.gcs.topic'
KAFKA_GCS_OUTPUT_FORMAT = 'kafka.gcs.output.format'
KAFKA_GCS_OUPUT_MODE = 'kafka.gcs.output.mode'
KAFKA_GCS_TERMINATION_TIMEOUT = 'kafka.gcs.termination.timeout'
KAFKA_STARTING_OFFSET = 'kafka.gcs.starting.offset'
KAFKA_GCS_CHECKPOINT_LOCATION = 'kafka.gcs.checkpoint.location'

# Pubsublite To GCS
PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL = "pubsublite.to.gcs.input.subscription.url"
PUBSUBLITE_TO_GCS_WRITE_MODE = "pubsublite.to.gcs.write.mode"
PUBSUBLITE_TO_GCS_OUTPUT_LOCATION = "pubsublite.to.gcs.output.location"
PUBSUBLITE_TO_GCS_CHECKPOINT_LOCATION = "pubsublite.to.gcs.checkpoint.location"
PUBSUBLITE_TO_GCS_OUTPUT_FORMAT = "pubsublite.to.gcs.output.format"
PUBSUBLITE_TO_GCS_TIMEOUT = "pubsublite.to.gcs.timeout"
PUBSUBLITE_TO_GCS_PROCESSING_TIME = "pubsublite.to.gcs.processing.time"

# Pub/Sub Lite to Bigtable
PUBSUBLITE_BIGTABLE_SUBSCRIPTION_PATH = "pubsublite.bigtable.subscription.path"
PUBSUBLITE_BIGTABLE_STREAMING_TIMEOUT = "pubsublite.bigtable.streaming.timeout"
PUBSUBLITE_BIGTABLE_STREAMING_TRIGGER = "pubsublite.bigtable.streaming.trigger"
PUBSUBLITE_BIGTABLE_STREAMING_CHECKPOINT_PATH = "pubsublite.bigtable.streaming.checkpoint.path"
PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT = "pubsublite.bigtable.output.project"
PUBSUBLITE_BIGTABLE_OUTPUT_INSTANCE = "pubsublite.bigtable.output.instance"
PUBSUBLITE_BIGTABLE_OUTPUT_TABLE = "pubsublite.bigtable.output.table"
PUBSUBLITE_BIGTABLE_OUTPUT_COLUMN_FAMILIES = "pubsublite.bigtable.output.column.families"
PUBSUBLITE_BIGTABLE_OUTPUT_MAX_VERSIONS = "pubsublite.bigtable.output.max.versions"

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
