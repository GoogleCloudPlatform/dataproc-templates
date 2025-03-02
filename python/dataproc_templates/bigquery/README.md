## BigQuery to GCS

Template for exporting a BigQuery table to files in Google Cloud Storage. It supports writing JSON, CSV, Parquet and Avro formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for reading from BigQuery.

## Arguments
* `bigquery.gcs.input.table`: BigQuery Input table name (format: `project:dataset.table`)
* `bigquery.gcs.output.format`: Output file format (one of: avro,parquet,csv,json)
* `bigquery.gcs.output.location`: Cloud Storage location for output files (format: `gs://BUCKET/...`)
* `bigquery.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
#### Optional Arguments
* `bigquery.gcs.output.partition.column`: Partition column name to partition the final output in destination bucket
* `bigquery.gcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `bigquery.gcs.output.compression`: None
* `bigquery.gcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
* `bigquery.gcs.output.emptyvalue`: Sets the string representation of an empty value
* `bigquery.gcs.output.encoding`: Decodes the CSV files by the given encoding type
* `bigquery.gcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `bigquery.gcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
* `bigquery.gcs.output.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `bigquery.gcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `bigquery.gcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `bigquery.gcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `bigquery.gcs.output.nullvalue`: Sets the string representation of a null value
* `bigquery.gcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `bigquery.gcs.output.quoteall`: None
* `bigquery.gcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `bigquery.gcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `bigquery.gcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format

## Usage

```
$ python main.py --template BIGQUERYTOGCS --help

usage: main.py [-h]
               --bigquery.gcs.input.table BIGQUERY.GCS.INPUT.TABLE
               --bigquery.gcs.output.format {avro,parquet,csv,json}
               --bigquery.gcs.output.location BIGQUERY.GCS.OUTPUT.LOCATION
               [--bigquery.gcs.output.mode {overwrite,append,ignore,errorifexists}]
               [--bigquery.gcs.output.partition.column BIGQUERY.GCS.OUTPUT.PARTITION.COLUMN]
               [--bigquery.gcs.output.chartoescapequoteescaping BIGQUERY.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--bigquery.gcs.output.compression BIGQUERY.GCS.OUTPUT.COMPRESSION]
               [--bigquery.gcs.output.dateformat BIGQUERY.GCS.OUTPUT.DATEFORMAT]
               [--bigquery.gcs.output.emptyvalue BIGQUERY.GCS.OUTPUT.EMPTYVALUE]
               [--bigquery.gcs.output.encoding BIGQUERY.GCS.OUTPUT.ENCODING]
               [--bigquery.gcs.output.escape BIGQUERY.GCS.OUTPUT.ESCAPE]
               [--bigquery.gcs.output.escapequotes BIGQUERY.GCS.OUTPUT.ESCAPEQUOTES]
               [--bigquery.gcs.output.header BIGQUERY.GCS.OUTPUT.HEADER]
               [--bigquery.gcs.output.ignoreleadingwhitespace BIGQUERY.GCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--bigquery.gcs.output.ignoretrailingwhitespace BIGQUERY.GCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--bigquery.gcs.output.linesep BIGQUERY.GCS.OUTPUT.LINESEP]
               [--bigquery.gcs.output.nullvalue BIGQUERY.GCS.OUTPUT.NULLVALUE]
               [--bigquery.gcs.output.quote BIGQUERY.GCS.OUTPUT.QUOTE]
               [--bigquery.gcs.output.quoteall BIGQUERY.GCS.OUTPUT.QUOTEALL]
               [--bigquery.gcs.output.sep BIGQUERY.GCS.OUTPUT.SEP]
               [--bigquery.gcs.output.timestampformat BIGQUERY.GCS.OUTPUT.TIMESTAMPFORMAT]
               [--bigquery.gcs.output.timestampntzformat BIGQUERY.GCS.OUTPUT.TIMESTAMPNTZFORMAT]

options:
  -h, --help            show this help message and exit
  --bigquery.gcs.input.table BIGQUERY.GCS.INPUT.TABLE
                        BigQuery Input table name
  --bigquery.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --bigquery.gcs.output.location BIGQUERY.GCS.OUTPUT.LOCATION
                        Cloud Storage location for output files
  --bigquery.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --bigquery.gcs.output.partition.column BIGQUERY.GCS.OUTPUT.PARTITION.COLUMN
                        Partition column name to partition the final output in destination bucket
  --bigquery.gcs.output.chartoescapequoteescaping BIGQUERY.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --bigquery.gcs.output.compression BIGQUERY.GCS.OUTPUT.COMPRESSION
  --bigquery.gcs.output.dateformat BIGQUERY.GCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --bigquery.gcs.output.emptyvalue BIGQUERY.GCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --bigquery.gcs.output.encoding BIGQUERY.GCS.OUTPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --bigquery.gcs.output.escape BIGQUERY.GCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --bigquery.gcs.output.escapequotes BIGQUERY.GCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --bigquery.gcs.output.header BIGQUERY.GCS.OUTPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --bigquery.gcs.output.ignoreleadingwhitespace BIGQUERY.GCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --bigquery.gcs.output.ignoretrailingwhitespace BIGQUERY.GCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --bigquery.gcs.output.linesep BIGQUERY.GCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --bigquery.gcs.output.nullvalue BIGQUERY.GCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --bigquery.gcs.output.quote BIGQUERY.GCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --bigquery.gcs.output.quoteall BIGQUERY.GCS.OUTPUT.QUOTEALL
  --bigquery.gcs.output.sep BIGQUERY.GCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --bigquery.gcs.output.timestampformat BIGQUERY.GCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --bigquery.gcs.output.timestampntzformat BIGQUERY.GCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
```
## Example submission

```
export GCP_PROJECT=my-project
export GCS_STAGING_LOCATION="gs://my-bucket"
export REGION=us-central1

./bin/start.sh \
-- --template=BIGQUERYTOGCS \
    --bigquery.gcs.input.table=python_templates_dataset.gcs_bq_table \
    --bigquery.gcs.output.format=csv \
    --bigquery.gcs.output.mode=overwrite \
    --bigquery.gcs.output.location="gs://my-output-bucket/csv/" \
    --bigquery.gcs.output.header=false \
    --bigquery.gcs.output.timestampntzformat="yyyy-MM-dd HH:mm:ss"
```

## BigQuery to Memorystore

Template for exporting data from BigQuery to Google Cloud Memorystore (Redis). This template supports writing data using hash and binary persistence model. It also supports specifying ttl for data, key column and automatic schema conversion & creation.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for reading from BigQuery and [Spark-Redis](https://github.com/RedisLabs/spark-redis) for writing to Redis.


## Arguments

* `bigquery.memorystore.input.table`: BigQuery Input table name (format: `project.dataset.table`)
* `bigquery.memorystore.output.host`: Redis Memorystore host
* `bigquery.memorystore.output.table`: Redis Memorystore target table name
* `bigquery.memorystore.output.key.column`: Redis Memorystore key column for target table

####   Optional Arguments

* `bigquery.memorystore.output.port`: Redis Memorystore port. Defaults to 6379
* `bigquery.memorystore.output.model`: Memorystore persistence model for Dataframe (one of: hash, binary) (Defaults to hash)
* `bigquery.memorystore.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
* `bigquery.memorystore.output.ttl`: Data time to live in seconds. Data doesn't expire if ttl is less than 1 (Defaults to 0)
* `bigquery.memorystore.output.dbnum`: Database / namespace for logical key separation (Defaults to 0)

## Usage
```
python main.py --template BIGQUERYTOMEMORYSTORE --help

usage: main.py [-h]
               --bigquery.memorystore.input.table BIGQUERY.MEMORYSTORE.INPUT.TABLE
               --bigquery.memorystore.output.host BIGQUERY.MEMORYSTORE.OUTPUT.HOST
               --bigquery.memorystore.output.table BIGQUERY.MEMORYSTORE.OUTPUT.TABLE
               --bigquery.memorystore.output.key.column BIGQUERY.MEMORYSTORE.OUTPUT.KEY.COLUMN
               [--bigquery.memorystore.output.port BIGQUERY.MEMORYSTORE.OUTPUT.PORT]
               [--bigquery.memorystore.output.model {hash,binary}]
               [--bigquery.memorystore.output.mode {overwrite,append,ignore,errorifexists}]
               [--bigquery.memorystore.output.ttl BIGQUERY.MEMORYSTORE.OUTPUT.TTL]
               [--bigquery.memorystore.output.dbnum BIGQUERY.MEMORYSTORE.OUTPUT.DBNUM]

options:
-h, --help            show this help message and exit
--bigquery.memorystore.input.table BIGQUERY.MEMORYSTORE.INPUT.TABLE
                        BigQuery Input table name
--bigquery.memorystore.output.host BIGQUERY.MEMORYSTORE.OUTPUT.HOST
                        Redis Memorystore host
--bigquery.memorystore.output.table BIGQUERY.MEMORYSTORE.OUTPUT.TABLE
                        Redis Memorystore target table name
--bigquery.memorystore.output.key.column BIGQUERY.MEMORYSTORE.OUTPUT.KEY.COLUMN
                        Redis Memorystore key column for target table
--bigquery.memorystore.output.port BIGQUERY.MEMORYSTORE.OUTPUT.PORT
                        Redis Memorystore port. Defaults to 6379
--bigquery.memorystore.output.model {hash,binary}
                        Memorystore persistence model for Dataframe (one of: hash, binary) (Defaults to hash)
--bigquery.memorystore.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
--bigquery.memorystore.output.ttl BIGQUERY.MEMORYSTORE.OUTPUT.TTL
                        Data time to live in seconds. Data doesn't expire if ttl is less than 1 (Defaults to 0)
--bigquery.memorystore.output.dbnum BIGQUERY.MEMORYSTORE.OUTPUT.DBNUM
                        Database / namespace for logical key separation (Defaults to 0)
```

##   Example submission

```
export GCP_PROJECT=myprojectid
export REGION=us-west1
export SUBNET=projects/myprojectid/regions/us-west1/subnetworks/mysubnetid
export GCS_STAGING_LOCATION="gs://python-dataproc-templates"
export JARS="gs://mygcsstagingbkt/jars/spark-redis_2.12-3.0.0-jar-with-dependencies.jar"

./bin/start.sh \
-- --template=BIGQUERYTOMEMORYSTORE \
--bigquery.memorystore.input.table=bigquery-public-data.fcc_political_ads.file_history \
--bigquery.memorystore.output.host=10.0.0.17 \
--bigquery.memorystore.output.port=6379 \
--bigquery.memorystore.output.table=file_history \
--bigquery.memorystore.output.key.column=fileHistoryId \
--bigquery.memorystore.output.model=hash \
--bigquery.memorystore.output.mode=overwrite \
--bigquery.memorystore.output.ttl=360 \
--bigquery.memorystore.output.dbnum=0
```

## Known limitations
With Spark-Redis, the Hash model does not support nested fields in the DataFrame. Alternatively, you can use the Binary persistence model, which supports nested fields.