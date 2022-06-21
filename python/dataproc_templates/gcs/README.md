## 1. GCS To BigQuery

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

## 2. Text To BigQuery

Template for reading TEXT files from Google Cloud Storage and writing them to a BigQuery table. It supports reading Text files with compression GZIP, BZIP2, LZ4, DEFLATE, NONE.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `text.bigquery.input.location`: GCS location of the input text files (format: `gs://BUCKET/...`)
* `text.bigquery.output.dataset`: BigQuery dataset for the output table
* `text.bigquery.output.table`: BigQuery output table name
* `text.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `text.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `text.bigquery.input.compression`: Input file compression format (one of: gzip,bzip4,lz4,deflate,none)
* `text.bigquery.input.delimiter`: Input file delimiter

## Usage

```
$ python main.py --template TEXTTOBIGQUERY --help

usage: main.py --template TEXTTOBIGQUERY [-h] \
    --text.bigquery.input.location TEXT.BIGQUERY.INPUT.LOCATION \
    --text.bigquery.output.dataset TEXT.BIGQUERY.OUTPUT.DATASET \
    --text.bigquery.output.table TEXT.BIGQUERY.OUTPUT.TABLE \
    --text.bigquery.temp.bucket.name TEXT.BIGQUERY.TEMP.BUCKET.NAME \
    --text.bigquery.input.compression {gzip,bzip4,lz4,deflate,none} \
    --text.bigquery.input.delimiter 
    [--text.bigquery.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --text.bigquery.input.location TEXT.BIGQUERY.INPUT.LOCATION
                        GCS location of the input files
  --text.bigquery.output.dataset TEXT.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --text.bigquery.output.table TEXT.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --text.bigquery.input.compression {gzip,bzip4,lz4,deflate,none}
                        Input file compression format (one of: gzip,bzip4,lz4,deflate,none)
  --text.bigquery.temp.bucket.name TEXT.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --text.bigquery.output.mode {overwrite,append,ignore,errorifexists}
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
-- --template=TEXTTOBIGQUERY \
    --text.bigquery.input.compression="<gzip|bzip4|lz4|deflate|none>" \
    --text.bigquery.input.delimiter="<delimiter>" \
    --text.bigquery.input.location="<gs://bucket/path>" \
    --text.bigquery.output.dataset="<dataset>" \
    --text.bigquery.output.table="<table>" \
    --text.bigquery.output.mode=<append|overwrite|ignore|errorifexists> \
    --text.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```
