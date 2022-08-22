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

# Hive To GCS

Template for reading data from Hive and writing to a GCS location. It supports reading from hive table.

## Arguments

* `spark.hadoop.hive.metastore.uris`: Hive metastore URI
* `hive.gcs.input.database`: Hive database for input table
* `hive.gcs.input.table`: Hive input table name
* `hive.gcs.output.location`: GCS location for output files (format: `gs://BUCKET/...`)
* `hive.gcs.output.format`: Output file format (one of: avro,parquet,csv,json) (Defaults to parquet)
* `hive.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)

## Usage

```
$ python main.py --template HIVETOGCS --help

usage: main.py --template HIVETOGCS [-h] \
    --hive.gcs.input.database HIVE.GCS.INPUT.DATABASE \ 
    --hive.gcs.input.table HIVE.GCS.INPUT.TABLE \
    --hive.gcs.output.location HIVE.GCS.OUTPUT.LOCATION \
    [--hive.gcs.output.format {avro,parquet,csv,json}] \
    [--hive.gcs.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --hive.gcs.input.database HIVE.GCS.INPUT.DATABASE
                        Hive database for exporting data to GCS
  --hive.gcs.input.table HIVE.GCS.INPUT.TABLE
                        Hive table for exporting data to GCS
  --hive.gcs.output.location HIVE.GCS.OUTPUT.LOCATION
                        GCS location for output files
  --hive.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json) (Defaults to parquet)
  --hive.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to overwrite)
```

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export SUBNET=<subnet>

./bin/start.sh \
    --properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083 \
    -- --template=HIVETOGCS \
    --hive.gcs.input.database="<database>" \
    --hive.gcs.input.table="<table>" \
    --hive.gcs.output.location="<gs://bucket/path>" \
    --hive.gcs.output.format="<csv|parquet|avro|json>" \
    --hive.gcs.output.mode="<append|overwrite|ignore|errorifexists>"
```
