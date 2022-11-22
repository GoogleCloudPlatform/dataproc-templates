# Hive Spark DDL Extractor To BigQuery

This is a utility to extract DDLs from Hive metastore. Users can use this to accelerate the Hive migration journey.

It uses [Spark SQL & Hive Integration](https://cloud.google.com/architecture/using-apache-hive-on-cloud-dataproc#querying_hive_with_sparksql) for querying Hive with SparkSQL.

## Arguments

* `spark.hadoop.hive.metastore.uris`: Hive metastore URI
* `hivesparkddl.bigquery.input.database`: Hive database for input table
* `hivesparkddl.bigquery.output.dataset`: BigQuery dataset for the output table
* `hivesparkddl.bigquery.output.table`: BigQuery output table name
* `hivesparkddl.bigquery.output.bucket`: Output bucket

## Usage

```
$ python main.py --template HIVESPARKDDLTOBQ --help

usage: main.py --template HIVESPARKDDLTOBQ [-h] \
    --hivesparkddl.bigquery.input.database HIVE.BIGQUERY.INPUT.DATABASE \
    --hivesparkddl.bigquery.output.dataset HIVE.BIGQUERY.OUTPUT.DATASET \
    --hivesparkddl.bigquery.output.table HIVE.BIGQUERY.OUTPUT.TABLE \
    --hivesparkddl.bigquery.temp.bucket.name HIVE.BIGQUERY.OUTPUT.BUCKET 

optional arguments:
  -h, --help            show this help message and exit
  --hive.bigquery.input.database HIVE.BIGQUERY.INPUT.DATABASE
                        Hive database for importing DDL to BigQuery
  --hive.bigquery.output.dataset HIVE.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --hive.bigquery.output.table HIVE.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --hive.bigquery.temp.bucket.name HIVE.BIGQUERY.OUTPUT.BUCKET.
                        Output bucket
```



## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> w
export SUBNET=<subnet>
./bin/start.sh \
    --properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083 \
    -- --template=HIVESPARKDDLTOBQ \
    --hivesparkddl.bigquery.input.database="<database>" \
    --hivesparkddl.bigquery.output.dataset="<dataset>" \
    --hivesparkddl.bigquery.output.table="<table>" \
    --hivesparkddl.bigquery.output.bucket="<bucket-name>"
```
