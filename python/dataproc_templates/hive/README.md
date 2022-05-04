## Hive To BigQuery

Template for reading data from Hive and writing them to a BigQuery table. It supports reading using hive sql query.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `spark.hadoop.hive.metastore.uris`: Hive metastore URI
* `hive.bigquery.sql`: Hive SQL query for required data
* `hive.bigquery.output.dataset`: BigQuery dataset for the output table
* `hive.bigquery.output.table`: BigQuery output table name
* `hive.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `hive.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)

## Usage

```
$ python main.py --template HIVETOBIGQUERY --help

usage: main.py --template HIVETOBIGQUERY [-h] \
    --hive.bigquery.sql HIVE.BIGQUERY.SQL \ 
    --hive.bigquery.output.dataset HIVE.BIGQUERY.OUTPUT.DATASET \
    --hive.bigquery.output.table HIVE.BIGQUERY.OUTPUT.TABLE \
    --hive.bigquery.temp.bucket.name HIVE.BIGQUERY.TEMP.BUCKET.NAME \ 
    [--hive.bigquery.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --hive.bigquery.sql HIVE.BIGQUERY.SQL
                        Hive sql for importing data to BigQuery
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
    --hive.bigquery.sql="<hive-sql>" \
    --hive.bigquery.output.dataset="<dataset>" \
    --hive.bigquery.output.table="<table>" \
    --hive.bigquery.output.mode="<append|overwrite|ignore|errorifexists>" \
    --hive.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```
