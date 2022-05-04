## Hive To BigQuery

Template for reading data from Hive and writing them to a BigQuery table. It supports reading from hive table.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `spark.hadoop.hive.metastore.uris`: Hive metastore URI
* `hive.bigquery.input.database`: Hive database for input table query for required data
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
export SUBNET=<subnet>

./bin/start.sh \
    --properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083 \
    -- --template=HIVETOBIGQUERY \
    --hive.bigquery.input.database="<database>" \
    --hive.bigquery.input.table="<table>" \
    --hive.bigquery.output.dataset="<dataset>" \
    --hive.bigquery.output.table="<table>" \
    --hive.bigquery.output.mode="<append|overwrite|ignore|errorifexists>" \
    --hive.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```
