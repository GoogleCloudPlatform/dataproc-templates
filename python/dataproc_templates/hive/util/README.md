# Hive DDL Extractor

This is a PySpark utility to extract DDLs of all the tables from a HIVE database using Hive metastore. Users can use this utility to accelerate their Hive migration journey.

## Arguments

* `spark.hadoop.hive.metastore.uris`: Hive metastore URI
* `hive.ddl.extractor.input.database`: Hive database for input table
* `hive.ddl.extractor.output.bucket`: Output bucket
* `hive.ddl.extractor.output.path`: Output GCS path

## Usage

```
$ python main.py --template HIVEDDLEXTRACTOR --help

usage: main.py --template HIVEDDLEXTRACTOR [-h] \
    --hive.ddl.extractor.input.database HIVE.DDL.EXTRACTOR.INPUT.DATABASE \
    --hive.ddl.extractor.output.bucket HIVE.DDL.EXTRACTOR.OUTPUT.BUCKET \
    --hive.ddl.extractor.output.path HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH

optional arguments:
  -h, --help            show this help message and exit
  --hive.ddl.extractor.input.database HIVE.DDL.EXTRACTOR.INPUT.DATABASE
                        Hive database for importing DDLs to GCS
  --hive.ddl.extractor.output.bucket HIVE.DDL.EXTRACTOR.OUTPUT.BUCKET.
                        Output GCS bucket to store Hive DDLs
   --hive.ddl.extractor.output.path HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH
                        GCS directory path e.g path/to/directory
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
    --hive.ddl.extractor.output.bucket="<bucket-name>" \
    --hive.ddl.extractor.output.path="<GCS-Path>"
```
