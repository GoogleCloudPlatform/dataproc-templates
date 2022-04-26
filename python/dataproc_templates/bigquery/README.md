## BigQuery to GCS

Template for exporting a BigQuery table to files in Google Cloud Storage. It supports writing JSON, CSV, Parquet and Avro formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for reading from BigQuery.

## Arguments

* `bigquery.gcs.input.table`: BigQuery Input table name (format: `project:dataset.table`)
* `bigquery.gcs.output.format`: Output file format (one of: avro,parquet,csv,json)
* `bigquery.gcs.output.location`: GCS location for output files (format: `gs://BUCKET/...`)
* `bigquery.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)

## Usage

```
$ python main.py --template BIGQUERYTOGCS --help

usage: main.py --template BIGQUERYTOGCS [-h] \
	--bigquery.gcs.input.table BIGQUERY.GCS.INPUT.TABLE \
	--bigquery.gcs.output.location BIGQUERY.GCS.OUTPUT.LOCATION \
	--bigquery.gcs.output.format {avro,parquet,csv,json} \
    [--bigquery.gcs.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --bigquery.gcs.input.table BIGQUERY.GCS.INPUT.TABLE
                        BigQuery Input table name
  --bigquery.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --bigquery.gcs.output.location BIGQUERY.GCS.OUTPUT.LOCATION
                        GCS location for output files
  --bigquery.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=<project_id>
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export REGION=<region>

./bin/start.sh \
-- --template=BIGQUERYTOGCS \
	--bigquery.gcs.input.table=<projectId:datasetName.tableName> \
	--bigquery.gcs.output.format=<csv|parquet|avro|json> \
	--bigquery.gcs.output.mode=<overwrite|append|ignore|errorifexists> \
	--bigquery.gcs.output.location=<gs://bucket/path>
```
