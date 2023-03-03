Template for reading files from Cloud Pub/sub Lite and writing them to a BigQuery table.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `pubsublite.to.bq.input.subscription`: Cloud Storage location of the input files (format: `gs://bucket/...`)
* `pubsublite.to.bq.write.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `pubsublite.to.bq.output.dataset`: BigQuery output dataset name
* `pubsublite.to.bq.output.table`: BigQuery output table name
* `pubsublite.to.bq.bucket.name`: Temporary bucket for the Spark BigQuery connector

## Usage

```
$ python main.py --template PUBSUBLITETOBQ --help

usage: main.py --template PUBSUBTOBQ [-h] \
    --pubsublite.to.bq.input.subscription PUBSUBLITE.BIGQUERY.INPUT.SUBSCRIPTION \
    --pubsublite.to.bq.project.id PUBSUBLITE.BIGQUERY.OUTPUT>PROJECT.ID \
    --pubsublite.to.bq.output.dataset PUBSUBLITE.BIGQUERY.OUTPUT.DATASET \
    --pubsublite.to.bq.output.table PUBSUBLITE.BIGQUERY.OUTPUT.TABLE \
    --pubsublite.to.bq.bucket.name PUBSUBLITE.BIGQUERY.BUCKET.NAME \
    [--pubsublite.to.bq.write.mode {overwrite,append,ignore,errorifexists}] \
    --pubsublite.to.bq.checkpoint.location PUBSUBLITE.BIGQUERY.CHECKPOINT.LOCATION
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-name> 
export JARS="gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

./bin/start.sh \
-- --template=PUBSUBLITETOBQ \
--pubsublite.to.bq.input.subscription=<pubsublite input subscription> \
--pubsublite.to.bq.project.id=<BQ project ID> \
--pubsublite.to.bq.output.dataset=<BQ dataset name> \
--pubsublite.to.bq.output.table=<BQ table name> \
--pubsublite.to.bq.write.mode=<Output write mode> \
--pubsublite.to.bq.bucket.name=<bucket name> \
--pubsublite.to.bq.checkpoint.location=<checkpoint folder location>
```


## Configurable Parameters

Following properties are available in commandline or [template.constants](../util/template_constants.py) file:

```
## Project that contains the input Pub/Sub subscription to be read
pubsubliteinput.project.id=<pubsub project id>
## PubSub subscription name
pubsubliteinput.subscription=<pubsub subscription>
## Stream timeout, for how long the subscription will be read
pubsublitetimeout.ms=60000
## Streaming duration, how often wil writes to BQ be triggered
pubsublitestreaming.duration.seconds=15
## Number of streams that will read from Pub/Sub subscription in parallel
pubsublitetotal.receivers=5
## Project that contains the output table
pubsublitebq.output.project.id=<pubsub to bq output project id>
## BigQuery output dataset
pubsublitebq.output.dataset=<bq output dataset>
## BigQuery output table
pubsublitebq.output.table=<bq output table>
## Number of records to be written per message to BigQuery
pubsublitebq.batch.size=1000
```