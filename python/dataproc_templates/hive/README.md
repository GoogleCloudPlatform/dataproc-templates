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
export GCP_PROJECT=my-project
export REGION=us-central1
export GCS_STAGING_LOCATION="gs://my-bucket"
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
export SUBNET=projects/my-project/regions/us-central1/subnetworks/test-subnet

./bin/start.sh \
    --properties=spark.hadoop.hive.metastore.uris=thrift://10.0.0.x:9083 \
    -- --template=HIVETOBIGQUERY \
    --hive.bigquery.input.database="default" \
    --hive.bigquery.input.table="employee" \
    --hive.bigquery.output.dataset="hive_to_bq_dataset" \
    --hive.bigquery.output.table="employee_out" \
    --hive.bigquery.output.mode="overwrite" \
    --hive.bigquery.temp.bucket.name="temp-bucket"
```

There are two optional properties as well with "Hive to BigQuery" Template. Please find below the details :-

```
--templateProperty hive.bigquery.temp.view.name='temporary_view_name' 
--templateProperty hive.bigquery.sql.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into BigQuery.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"


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
export GCP_PROJECT=my-project
export REGION=us-central1
export GCS_STAGING_LOCATION="gs://my-bucket"
export SUBNET=projects/my-project/regions/us-central1/subnetworks/test-subnet

./bin/start.sh \
    --properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083 \
    -- --template=HIVETOGCS \
    --hive.gcs.input.database="default" \
    --hive.gcs.input.table="employee" \
    --hive.gcs.output.location="gs://my-output-bucket/hive-gcs-output" \
    --hive.gcs.output.format="csv" \
    --hive.gcs.output.mode="overwrite"
```

There are two optional properties as well with "Hive to GCS" Template. Please find below the details :-

```
--templateProperty hive.gcs.temp.view.name='temporary_view_name' 
--templateProperty hive.gcs.sql.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into GCS.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"
