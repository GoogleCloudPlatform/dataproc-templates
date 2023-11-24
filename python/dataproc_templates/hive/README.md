# Hive To BigQuery

Template for reading data from Hive and writing to BigQuery table. It supports reading from hive table.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `spark.hadoop.hive.metastore.uris`: Hive metastore URI
* `hive.bigquery.input.database`: Hive database for input table
* `hive.bigquery.input.table`: Hive input table name
* `hive.bigquery.output.dataset`: BigQuery dataset for the output table
* `hive.bigquery.output.table`: BigQuery output table name
* `hive.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `hive.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)

## Usage

```
$ python main.py --template HIVETOBIGQUERY --help

usage: main.py --template HIVETOBIGQUERY [-h] \
    --hive.bigquery.input.database HIVE.BIGQUERY.INPUT.DATABASE \
    --hive.bigquery.input.table HIVE.BIGQUERY.INPUT.TABLE \
    --hive.bigquery.output.dataset HIVE.BIGQUERY.OUTPUT.DATASET \
    --hive.bigquery.output.table HIVE.BIGQUERY.OUTPUT.TABLE \
    --hive.bigquery.temp.bucket.name HIVE.BIGQUERY.TEMP.BUCKET.NAME \
    [--hive.bigquery.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --hive.bigquery.input.database HIVE.BIGQUERY.INPUT.DATABASE
                        Hive database for importing data to BigQuery
  --hive.bigquery.input.table HIVE.BIGQUERY.INPUT.TABLE
                        Hive table for importing data to BigQuery
  --hive.bigquery.output.dataset HIVE.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --hive.bigquery.output.table HIVE.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --hive.bigquery.temp.bucket.name HIVE.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --hive.bigquery.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to overwrite)
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=my-project
export REGION=us-central1
export GCS_STAGING_LOCATION="gs://my-bucket"
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
export SUBNET=projects/my-project/regions/us-central1/subnetworks/test-subnet

./bin/start.sh \
    --properties=spark.hadoop.hive.metastore.uris=thrift://10.0.0.x:9083 \
    -- --template=HIVETOBIGQUERY \
    --hive.bigquery.input.database="default" \
    --hive.bigquery.input.table="employee" \
    --hive.bigquery.output.dataset="hive_to_bq_dataset" \
    --hive.bigquery.output.table="employee_out" \
    --hive.bigquery.output.mode="overwrite" \
    --hive.bigquery.temp.bucket.name="temp-bucket"
```

There are two optional properties as well with "Hive to BigQuery" Template. Please find below the details :-

```
--templateProperty hive.bigquery.temp.view.name='temporary_view_name'
--templateProperty hive.bigquery.sql.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into BigQuery.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"


# Hive To Cloud Storage

Template for reading data from Hive and writing to a Cloud Storage location. It supports reading from Hive table.

## Arguments
* `spark.hadoop.hive.metastore.uris`: Hive metastore URI
* `hive.gcs.input.database`: Hive database for input table
* `hive.gcs.input.table`: Hive input table name
* `hive.gcs.output.location`: Cloud Storage location for output files (format: `gs://BUCKET/...`)
* `hive.gcs.output.format`: Output file format (one of: avro,parquet,csv,json) (Defaults to parquet)
* `hive.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
#### Optional Arguments
* `hive.gcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `hive.gcs.output.compression`: Compression codec to use when saving to file. This can be one of the known case-insensitive short names (none, bzip2, gzip, lz4, snappy and deflate)
* `hive.gcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
* `hive.gcs.output.emptyvalue`: Sets the string representation of an empty value
* `hive.gcs.output.encoding`: Specifies encoding (charset) of saved CSV files
* `hive.gcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `hive.gcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
* `hive.gcs.output.header`: Writes the names of columns as the first line. Defaults to True
* `hive.gcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `hive.gcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `hive.gcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `hive.gcs.output.nullvalue`: Sets the string representation of a null value
* `hive.gcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For writing, if an empty string is set, it uses u0000 (null character)
* `hive.gcs.output.quoteall`: A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character
* `hive.gcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `hive.gcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `hive.gcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format

## Usage

```
$ python main.py --template HIVETOGCS --help

usage: main.py [-h]
               --hive.gcs.input.database HIVE.GCS.INPUT.DATABASE
               --hive.gcs.input.table HIVE.GCS.INPUT.TABLE
               --hive.gcs.output.location HIVE.GCS.OUTPUT.LOCATION
               [--hive.gcs.output.format {avro,parquet,csv,json}]
               [--hive.gcs.output.mode {overwrite,append,ignore,errorifexists}]
               [--hive.gcs.temp.view.name HIVE.GCS.TEMP.VIEW.NAME]
               [--hive.gcs.sql.query HIVE.GCS.SQL.QUERY]
               [--hive.gcs.output.chartoescapequoteescaping HIVE.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--hive.gcs.output.compression HIVE.GCS.OUTPUT.COMPRESSION]
               [--hive.gcs.output.dateformat HIVE.GCS.OUTPUT.DATEFORMAT]
               [--hive.gcs.output.emptyvalue HIVE.GCS.OUTPUT.EMPTYVALUE]
               [--hive.gcs.output.encoding HIVE.GCS.OUTPUT.ENCODING]
               [--hive.gcs.output.escape HIVE.GCS.OUTPUT.ESCAPE]
               [--hive.gcs.output.escapequotes HIVE.GCS.OUTPUT.ESCAPEQUOTES]
               [--hive.gcs.output.header HIVE.GCS.OUTPUT.HEADER]
               [--hive.gcs.output.ignoreleadingwhitespace HIVE.GCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--hive.gcs.output.ignoretrailingwhitespace HIVE.GCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--hive.gcs.output.linesep HIVE.GCS.OUTPUT.LINESEP]
               [--hive.gcs.output.nullvalue HIVE.GCS.OUTPUT.NULLVALUE]
               [--hive.gcs.output.quote HIVE.GCS.OUTPUT.QUOTE]
               [--hive.gcs.output.quoteall HIVE.GCS.OUTPUT.QUOTEALL]
               [--hive.gcs.output.sep HIVE.GCS.OUTPUT.SEP]
               [--hive.gcs.output.timestampformat HIVE.GCS.OUTPUT.TIMESTAMPFORMAT]
               [--hive.gcs.output.timestampntzformat HIVE.GCS.OUTPUT.TIMESTAMPNTZFORMAT]

options:
  -h, --help            show this help message and exit
  --hive.gcs.input.database HIVE.GCS.INPUT.DATABASE
                        Hive database for exporting data to GCS
  --hive.gcs.input.table HIVE.GCS.INPUT.TABLE
                        Hive table for exporting data to GCS
  --hive.gcs.output.location HIVE.GCS.OUTPUT.LOCATION
                        Cloud Storage location for output files
  --hive.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json) (Defaults to parquet)
  --hive.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to overwrite)
  --hive.gcs.temp.view.name HIVE.GCS.TEMP.VIEW.NAME
                        Temp view name for creating a spark sql view on source data. This name has to match with the table name that will be used in the SQL query
  --hive.gcs.sql.query HIVE.GCS.SQL.QUERY
                        SQL query for data transformation. This must use the temp view name as the table to query from.
  --hive.gcs.output.chartoescapequoteescaping HIVE.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --hive.gcs.output.compression HIVE.GCS.OUTPUT.COMPRESSION
  --hive.gcs.output.dateformat HIVE.GCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --hive.gcs.output.emptyvalue HIVE.GCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --hive.gcs.output.encoding HIVE.GCS.OUTPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --hive.gcs.output.escape HIVE.GCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --hive.gcs.output.escapequotes HIVE.GCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --hive.gcs.output.header HIVE.GCS.OUTPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --hive.gcs.output.ignoreleadingwhitespace HIVE.GCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --hive.gcs.output.ignoretrailingwhitespace HIVE.GCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --hive.gcs.output.linesep HIVE.GCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --hive.gcs.output.nullvalue HIVE.GCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --hive.gcs.output.quote HIVE.GCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --hive.gcs.output.quoteall HIVE.GCS.OUTPUT.QUOTEALL
  --hive.gcs.output.sep HIVE.GCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --hive.gcs.output.timestampformat HIVE.GCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --hive.gcs.output.timestampntzformat HIVE.GCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
```

## Example submission

```
export GCP_PROJECT=my-project
export REGION=us-central1
export GCS_STAGING_LOCATION="gs://my-bucket"
export SUBNET=projects/my-project/regions/us-central1/subnetworks/test-subnet

./bin/start.sh \
    --properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083 \
    -- --template=HIVETOGCS \
    --hive.gcs.input.database="default" \
    --hive.gcs.input.table="employee" \
    --hive.gcs.output.location="gs://my-output-bucket/hive-gcs-output" \
    --hive.gcs.output.format="csv" \
    --hive.gcs.output.mode="overwrite"
```

There are two optional properties as well with "Hive to GCS" Template. Please find below the details :-

```
--templateProperty hive.gcs.temp.view.name='temporary_view_name'
--templateProperty hive.gcs.sql.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into Cloud Storage.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"
