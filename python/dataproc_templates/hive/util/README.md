# Hive DDL Extractor

This is a utility to extract DDLs from Hive metastore. Users can use this to accelerate the Hive migration journey.

It uses [Spark SQL & Hive Integration](https://cloud.google.com/architecture/using-apache-hive-on-cloud-dataproc#querying_hive_with_sparksql) for querying Hive with SparkSQL.

## Arguments

* `spark.hadoop.hive.metastore.uris`: Hive metastore URI
* `hive.ddl.extractor.input.database`: Hive database for input table
* `hive.ddl.extractor.output.dataset`: BigQuery dataset for the output table
* `hive.ddl.extractor.output.table`: BigQuery output table name
* `hive.ddl.extractor.output.bucket`: Output bucket

## Usage

```
$ python main.py --template HIVESPARKDDLTOBQ --help

usage: main.py --template HIVESPARKDDLTOBQ [-h] \
    --hive.ddl.extractor.input.database HIVE.BIGQUERY.INPUT.DATABASE \
    --hive.ddl.extractor.output.dataset HIVE.BIGQUERY.OUTPUT.DATASET \
    --hive.ddl.extractor.output.table HIVE.BIGQUERY.OUTPUT.TABLE \
    --hive.ddl.extractor.temp.bucket.name HIVE.BIGQUERY.OUTPUT.BUCKET 

optional arguments:
  -h, --help            show this help message and exit
  --hive.ddl.extractor.input.database HIVE.BIGQUERY.INPUT.DATABASE
                        Hive database for importing DDL to BigQuery
  --hive.ddl.extractor.output.dataset HIVE.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --hive.ddl.extractor.output.table HIVE.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --hive.ddl.extractor.temp.bucket.name HIVE.BIGQUERY.OUTPUT.BUCKET.
                        Output bucket
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export SUBNET=<subnet>
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
./bin/start.sh \
    --properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083 \
    -- --template=HIVEDDLEXTRACTOR \
    --hive.ddl.extractor.input.database="<database>" \
    --hive.ddl.extractor.output.dataset="<dataset>" \
    --hive.ddl.extractor.output.table="<table>" \
    --hive.ddl.extractor.output.bucket="<bucket-name>"
```
