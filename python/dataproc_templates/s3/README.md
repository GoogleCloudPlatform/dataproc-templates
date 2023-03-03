# Amazon S3 To BigQuery

Template for reading files from Amazon S3 and writing them to a BigQuery table. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `s3.bq.input.location` : Amazon S3 input location. Input location must begin with `s3a://`
* `s3.bq.access.key` : Access key to access Amazon S3 bucket
* `s3.bq.secret.key` : Secret key to access Amazon S3 bucket
* `s3.bq.input.format` : Input file format in Amazon S3 bucket (one of : avro, parquet, csv, json)
* `s3.bq.output.dataset.name` : BigQuery dataset for the output table
* `s3.bq.output.table.name` : BigQuery output table name
* `s3.bq.temp.bucket.name` : Pre existing GCS bucket name where temporary files are staged
* `s3.bq.output.mode` : (Optional) Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)

## Usage

```bash
$ python main.py --template S3TOBIGQUERY --help

usage: main.py --template S3TOBIGQUERY [-h] \
    --s3.bq.input.location S3.BQ.INPUT.LOCATION \
    --s3.bq.access.key S3.BQ.ACCESS.KEY \
    --s3.bq.secret.key S3.BQ.SECRET.KEY \
    --s3.bq.input.format {avro, parquet, csv, json} \
    --s3.bq.output.dataset.name S3.BQ.OUTPUT.DATASET.NAME \
    --s3.bq.output.table.name S3.BQ.OUTPUT.TABLE.NAME \
    --s3.bq.temp.bucket.name S3.BQ.TEMP.BUCKET.NAME

optional arguments:
  -h, --help            show this help message and exit
  --s3.bq.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## General execution

```bash
export GCP_PROJECT=<project-id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-bucket-name> 
export JARS=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar 

./bin/start.sh \
-- --template=S3TOBIGQUERY \
    --s3.bq.input.location="s3a://<s3-input-path>" \
    --s3.bq.access.key="<s3-access-key>" \
    --s3.bq.secret.key="<s3-secret-key>" \
    --s3.bq.input.format="<avro,parquet,csv,json>" \
    --s3.bq.output.dataset.name="<bq-dataset-name>" \
    --s3.bq.output.table.name="<bq-table-name>" \
    --s3.bq.output.mode="<overwrite,append,ignore,errorifexists>" \
    --s3.bq.temp.bucket.name="<temp-gcs-bucket-name>"
```

## Example submission

```bash
export GCP_PROJECT=my-project
export REGION=us-west1
export GCS_STAGING_LOCATION=my-staging-bucket 
export JARS=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar 

./bin/start.sh \
-- --template=S3TOBIGQUERY \
    --s3.bq.input.location="s3a://dataproctemplates-bucket/cities.csv" \
    --s3.bq.access.key="SomeAccessKey6789PQR" \
    --s3.bq.secret.key="SomeSecretKey123XYZ" \
    --s3.bq.input.format="csv" \
    --s3.bq.output.dataset.name="dataproc_templates" \
    --s3.bq.output.table.name="s3_to_bq" \
    --s3.bq.output.mode="overwrite" \
    --s3.bq.temp.bucket.name="temp-bucket-for-files"
```
