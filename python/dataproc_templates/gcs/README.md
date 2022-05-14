## GCS To BigQuery

Template for reading files from Google Cloud Storage and writing them to a BigQuery table. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `gcs.bigquery.input.location`: GCS location of the input files (format: `gs://BUCKET/...`)
* `gcs.bigquery.output.dataset`: BigQuery dataset for the output table
* `gcs.bigquery.output.table`: BigQuery output table name
* `gcs.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json)
* `gcs.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `gcs.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)

## Usage

```
$ python main.py --template GCSTOBIGQUERY --help

usage: main.py --template GCSTOBIGQUERY [-h] \
    --gcs.bigquery.input.location GCS.BIGQUERY.INPUT.LOCATION \
    --gcs.bigquery.output.dataset GCS.BIGQUERY.OUTPUT.DATASET \
    --gcs.bigquery.output.table GCS.BIGQUERY.OUTPUT.TABLE \
    --gcs.bigquery.input.format {avro,parquet,csv,json} \
    --gcs.bigquery.temp.bucket.name GCS.BIGQUERY.TEMP.BUCKET.NAME \
    [--gcs.bigquery.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --gcs.bigquery.input.location GCS.BIGQUERY.INPUT.LOCATION
                        GCS location of the input files
  --gcs.bigquery.output.dataset GCS.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --gcs.bigquery.output.table GCS.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --gcs.bigquery.input.format {avro,parquet,csv,json}
                        Input file format (one of: avro,parquet,csv,json)
  --gcs.bigquery.temp.bucket.name GCS.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --gcs.bigquery.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

./bin/start.sh \
-- --template=GCSTOBIGQUERY \
    --gcs.bigquery.input.format="<json|csv|parquet|avro>" \
    --gcs.bigquery.input.location="<gs://bucket/path>" \
    --gcs.bigquery.output.dataset="<dataset>" \
    --gcs.bigquery.output.table="<table>" \
    --gcs.bigquery.output.mode=<append|overwrite|ignore|errorifexists> \
    --gcs.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```

## GCS To Spanner

Template for reading files from Google Cloud Storage and writing them to a
Spanner table. It supports reading Parquet, Avro and ORC formats.

## Arguments

* `gcs.spanner.input.location`: GCS location of the input file
(format: `gs://BUCKET/...`)
* `gcs.spanner.input.format`: Input file format (one of: avro, parquet, orc)
* `gcs.spanner.output.instance`: Spanner instance name
* `gcs.spanner.output.database`: Spanner database name
* `gcs.spanner.output.table`: Spanner table name
* `gcs.spanner.output.primary_key`: Primary Key of the Spanner table 
* `gcs.spanner.output.mode`: Output write mode
(one of: append, overwrite, ignore, errorifexists). Defaults to `errorifexists`
* `gcs.spanner.output.batch_size`: JDBC batch size

## Usage

```
$ python main.py --template GCSTSPANNER --help

usage: main.py [-h] \
    --gcs.spanner.input.location GCS.SPANNER.INPUT.LOCATION \
    --gcs.spanner.input.format {avro,parquet,orc} \
    --gcs.spanner.output.instance GCS.SPANNER.OUTPUT.INSTANCE \
    --gcs.spanner.output.database GCS.SPANNER.OUTPUT.DATABASE \
    --gcs.spanner.output.table GCS.SPANNER.OUTPUT.TABLE \
    --gcs.spanner.output.primary_key GCS.SPANNER.OUTPUT.PRIMARY_KEY \
    [--gcs.spanner.output.mode {overwrite,append,ignore,errorifexists}] \
    [--gcs.spanner.output.batch_size GCS.SPANNER.OUTPUT.BATCH_SIZE]

optional arguments:
  -h, --help            show this help message and exit
  --gcs.spanner.input.location GCS.SPANNER.INPUT.LOCATION
                        GCS location of the input files
  --gcs.spanner.input.format {avro,parquet,orc}
                        Input file format (one of: <avro,parquet,orc>)
  --gcs.spanner.output.instance GCS.SPANNER.OUTPUT.INSTANCE
                        Spanner output instance
  --gcs.spanner.output.database GCS.SPANNER.OUTPUT.DATABASE
                        Spanner output database
  --gcs.spanner.output.table GCS.SPANNER.OUTPUT.TABLE
                        Spanner output table
  --gcs.spanner.output.primary_key GCS.SPANNER.OUTPUT.PRIMARY_KEY
                        Primary key of the table
  --gcs.spanner.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: <Overwrite|ErrorIfExists|Append|Ignore>) (Defaults to ErrorIfExists)
  --gcs.spanner.output.batch_size GCS.SPANNER.OUTPUT.BATCH_SIZE
                        Spanner batch insert size

```

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 

./bin/start.sh \
-- --template=GCSTOSPANNER \
    --gcs.spanner.input.location="<gs://bucket/path>" \
    --gcs.spanner.input.format="<parquet|avro|orc>" \
    --gcs.spanner.output.instance="<instance>" \
    --gcs.spanner.output.database="<database>" \
    --gcs.spanner.output.table="<table_name>" \
    --gcs.spanner.output.primary_key="<primary_key>" \
    --gcs.spanner.output.mode=<append|overwrite|ignore|errorifexists> \
    --gcs.spanner.output.batch_size="<batch_size>"
```
