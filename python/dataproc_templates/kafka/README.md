# Kafka To Cloud Storage

Template for reading files from Kafka topic and writing them to a Cloud Storage bucket. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the Spark-Sql Kafka jars to write streaming data from Kafka topic to Cloud Storage .

## Required JAR files

  -  [Kafka 0.10+ Source For Structured Streaming](https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.13/3.2.0)

   
## Arguments

* `kafka.gcs.checkpoint.location`: Cloud Storage location for storing checkpoints during transfer (format: `gs://bucket/...`)
* `kafka.gcs.output.location.gcs.path`: Output Cloud Storage Location for storing streaming data (format: `gs://bucket/...`)
* `kafka.gcs.bootstrap.servers`: List of kafka bootstrap servers (format: *'[x1.x2.x3.x4:port1,y1.y2.y3.y4:port2]')*
* `kafka.gcs.topic`: Topic names for respective kafka server
* `kafka.gcs.starting.offset`: Offset to start reading from. Accepted values: "earliest", "latest" (streaming only), or json string """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
* `kafka.gcs.output.format`: csv| json| parquet| avro
* `kafka.gcs.output.mode`: append|complete|update
* `kafka.gcs.termination.timeout`: timeout **(in seconds)**

## Optional Arguments

* `kafka.gcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `kafka.gcs.output.compression`: None
* `kafka.gcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
* `kafka.gcs.output.emptyvalue`: Sets the string representation of an empty value
* `kafka.gcs.output.encoding`: Decodes the CSV files by the given encoding type
* `kafka.gcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `kafka.gcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
* `kafka.gcs.output.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `kafka.gcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `kafka.gcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `kafka.gcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `kafka.gcs.output.nullvalue`: Sets the string representation of a null value
* `kafka.gcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `kafka.gcs.output.quoteall`: A flag indicating whether all values should always be enclosed in quotes.
* `kafka.gcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `kafka.gcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `kafka.gcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format

## Usage

```
$ python main.py --template KAFKATOGCS --help                
                        
usage: main.py [-h] --kafka.gcs.checkpoint.location KAFKA.GCS.CHECKPOINT.LOCATION --kafka.gcs.output.location.gcs.path KAFKA.GCS.OUTPUT.LOCATION.GCS.PATH
               --kafka.gcs.bootstrap.servers KAFKA.GCS.BOOTSTRAP.SERVERS 
               --kafka.gcs.topic KAFKA.GCS.TOPIC 
               --kafka.gcs.starting.offset KAFKA.GCS.STARTING.OFFSET
               --kafka.gcs.output.format KAFKA.GCS.OUTPUT.FORMAT 
               --kafka.gcs.output.mode {append,update,complete} 
               --kafka.gcs.termination.timeout KAFKA.GCS.TERMINATION.TIMEOUT 
               [--kafka.gcs.output.timestampntzformat KAFKA.GCS.OUTPUT.TIMESTAMPNTZFORMAT]
               [--kafka.gcs.output.emptyvalue KAFKA.GCS.OUTPUT.EMPTYVALUE] 
               [--kafka.gcs.output.nullvalue KAFKA.GCS.OUTPUT.NULLVALUE]
               [--kafka.gcs.output.chartoescapequoteescaping KAFKA.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING] 
               [--kafka.gcs.output.escape KAFKA.GCS.OUTPUT.ESCAPE]
               [--kafka.gcs.output.linesep KAFKA.GCS.OUTPUT.LINESEP] 
               [--kafka.gcs.output.ignoretrailingwhitespace KAFKA.GCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--kafka.gcs.output.quote KAFKA.GCS.OUTPUT.QUOTE]
               [--kafka.gcs.output.encoding KAFKA.GCS.OUTPUT.ENCODING]
               [--kafka.gcs.output.quoteall KAFKA.GCS.OUTPUT.QUOTEALL] 
               [--kafka.gcs.output.compression KAFKA.GCS.OUTPUT.COMPRESSION]
               [--kafka.gcs.output.escapequotes KAFKA.GCS.OUTPUT.ESCAPEQUOTES] 
               [--kafka.gcs.output.header KAFKA.GCS.OUTPUT.HEADER]
               [--kafka.gcs.output.sep KAFKA.GCS.OUTPUT.SEP] 
               [--kafka.gcs.output.dateformat KAFKA.GCS.OUTPUT.DATEFORMAT]
               [--kafka.gcs.output.ignoreleadingwhitespace KAFKA.GCS.OUTPUT.IGNORELEADINGWHITESPACE] 
               [--kafka.gcs.output.timestampformat KAFKA.GCS.OUTPUT.TIMESTAMPFORMAT]

optional arguments:
  -h, --help            show this help message and exit
  --kafka.gcs.checkpoint.location KAFKA.GCS.CHECKPOINT.LOCATION
                        Checkpoint location for Kafka to GCS Template
  --kafka.gcs.output.location.gcs.path KAFKA.GCS.OUTPUT.LOCATION.GCS.PATH
                        GCS location of the destination folder
  --kafka.gcs.bootstrap.servers KAFKA.GCS.BOOTSTRAP.SERVERS
                        Kafka topic address from where data is coming
  --kafka.gcs.topic KAFKA.GCS.TOPIC
                        Kafka Topic Name
  --kafka.gcs.starting.offset KAFKA.GCS.STARTING.OFFSET
                        Starting offset value (earliest, latest, json_string)
  --kafka.gcs.output.format KAFKA.GCS.OUTPUT.FORMAT
                        Ouput format of the data (json , csv, avro, parquet)
  --kafka.gcs.output.mode {append,update,complete}
                        Ouput write mode (append, update, complete)
  --kafka.gcs.termination.timeout KAFKA.GCS.TERMINATION.TIMEOUT
                        Timeout for termination of kafka subscription
  --kafka.gcs.output.timestampntzformat KAFKA.GCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
  --kafka.gcs.output.emptyvalue KAFKA.GCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --kafka.gcs.output.nullvalue KAFKA.GCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --kafka.gcs.output.chartoescapequoteescaping KAFKA.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters
                        are different, \0 otherwise
  --kafka.gcs.output.escape KAFKA.GCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --kafka.gcs.output.linesep KAFKA.GCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --kafka.gcs.output.ignoretrailingwhitespace KAFKA.GCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --kafka.gcs.output.quote KAFKA.GCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For writing, if an empty string is set, it uses
                        u0000 (null character)
  --kafka.gcs.output.encoding KAFKA.GCS.OUTPUT.ENCODING
                        Specifies encoding (charset) of saved CSV files
  --kafka.gcs.output.quoteall KAFKA.GCS.OUTPUT.QUOTEALL
                        A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character
  --kafka.gcs.output.compression KAFKA.GCS.OUTPUT.COMPRESSION
                        Compression codec to use when saving to file. This can be one of the known case-insensitive short names (none, bzip2, gzip, lz4, snappy and deflate)
  --kafka.gcs.output.escapequotes KAFKA.GCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --kafka.gcs.output.header KAFKA.GCS.OUTPUT.HEADER
                        Writes the names of columns as the first line. Defaults to True
  --kafka.gcs.output.sep KAFKA.GCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --kafka.gcs.output.dateformat KAFKA.GCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --kafka.gcs.output.ignoreleadingwhitespace KAFKA.GCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --kafka.gcs.output.timestampformat KAFKA.GCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
```

## Example submission

```
export GCP_PROJECT=<gcp-project>
export REGION=<region> 
export GCS_STAGING_LOCATION=<gcs-staging-location>
export SUBNET=<subnet>
export JARS="gs://{jar-bucket}/spark-sql-kafka-0-10_2.12-3.2.0.jar,gs://{jar-bucket}/kafka-clients-2.8.0.jar,gs://{jar-bucket}/commons-pool2-2.6.2.jar,gs://{jar-bucket}/spark-token-provider-kafka-0-10_2.12-3.2.0.jar"

./bin/start.sh \
-- --template=KAFKATOGCS \
  --kafka.gcs.checkpoint.location="<gcs checkpoint storage location>" \
  --kafka.gcs.output.location.gcs.path= "<gcs output location path>" \
   --kafka.gcs.bootstrap.servers="<list of kafka connections>" \
   --kafka.gcs.topic="<integration topics to subscribe>" \
   --kafka.gcs.starting.offset="<earliest|latest|json_offset>" \
   --kafka.gcs.output.format="{json|csv|avro|parquet}" \
   --kafka.gcs.output.mode="{append|overwrite}" \
   --kafka.gcs.termination.timeout="time in seconds"
```

# Kafka To BigQuery

Template for reading files from streaming Kafka topic and writing them to a BigQuery table.

It uses the 
  - [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

  - [ Kafka 0.10+ Source For Structured Streaming](https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10) for connecting list of broker connections.

## Arguments

* `kafka.to.bq.checkpoint.location`: Cloud Storage location for storing checkpoints during transfer (format: `gs://bucket/...`)
* `kafka.to.bq.bootstrap.servers`: List of kafka bootstrap servers (format: *'[x1.x2.x3.x4:port1,y1.y2.y3.y4:port2]')*
* `kafka.to.bq.topic`: Topic names for respective kafka server
* `kafka.to.bq.starting.offset`:  Offset to start reading from. Accepted values: "earliest", "latest" (streaming only), or json string """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
* `kafka.to.bq.dataset`: Temporary bucket for the Spark BigQuery connector
* `kafka.to.bq.table`:  name of the bigquery table (destination)
* `kafka.to.bq.output.mode`:  Output mode of the table (append, overwrite, update, complete)
* `kafka.to.bq.temp.bucket.name`: Name of bucket for temporary storage files (not location).
* `kafka.to.bq.termination.timeout`: **(in seconds)** Waits for specified time in sec before termination of stream 


## Usage

```
$ python main.py --template KAFKATOBQ --help

usage: main.py --template KAFKATOBQ [-h] \
    --kafka.to.bq.checkpoint.location KAFKA.BIGQUERY.CHEKPOINT.LOCATION \
    --kafka.to.bq.bootstrap.servers KAFKA.BOOTSTRAP.SERVERS \
    --kafka.to.bq.topic KAFKA.BIGQUERY.TOPIC \
    --kafka.to.bq.starting.offset KAFKA.BIGUERY.STARTING.OFFSET \
    --kafka.to.bq.dataset KAFKA.BQ.DATASET \
    --kafka.to.bq.table KAFKA.BQ.TABLE.NAME \
    --kafka.to.bq.output.mode KAFKA.BQ.OUTPUT.MODE \
    --kafka.to.bq.temp.bucket.name KAFKA.BIGQUERY.TEMP.BUCKET.NAME

```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example)  and [Kafka 0.10+ Source For Structured Streaming](https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10)  to be available in the Dataproc cluster.


## Example submission

```
-export GCP_PROJECT=<gcp-project>
-export REGION=<region> 
-export GCS_STAGING_LOCATION=<gcs-staging-location>
-export SUBNET=<subnet>
-export JARS="gs://{jar-bucket}/spark-sql-kafka-0-10_2.12-3.2.0.jar,gs://{jar-bucket}/kafka-clients-2.8.0.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar,gs://{jar-bucket}/commons-pool2-2.6.2.jar,gs://{jar-bucket}/spark-token-provider-kafka-0-10_2.12-3.2.0.jar"

-./bin/start.sh \
-- --template=KAFKATOBQ \
  --kafka.to.bq.checkpoint.location="<gcs checkpoint storage location>" \
   --kafka.to.bq.bootstrap.servers="<list of kafka connections>" \
   --kafka.to.bq.topic="<integration topics to subscribe>" \
   --kafka.to.bq.starting.offset="<earliest|latest|json_offset>" \
   --kafka.to.bq.dataset="<bigquery_dataset_name>" \
   --kafka.to.bq.table="<bigquery_table_name>" \
   --kafka.to.bq.temp.bucket.name="<bucket name for staging files>" \
   --kafka.to.bq.output.mode=<append|overwrite|update> \
   --kafka.to.bq.termination.timeout="time in seconds"
```

