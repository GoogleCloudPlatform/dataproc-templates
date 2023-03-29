## PubSubLite to GCS

Template for reading files from Pub/Sub Lite and writing them to Google Cloud Storage. It supports writing JSON, CSV, Parquet and Avro formats.


## Arguments

* `pubsublite.to.gcs.input.subscription.url`: PubSubLite Input Subscription Url
* `pubsublite.to.gcs.write.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `pubsublite.to.gcs.output.location`: GCS Location to put Output Files (format: `gs://BUCKET/...`)
* `pubsublite.to.gcs.checkpoint.location`: GCS Checkpoint Folder Location
* `pubsublite.to.gcs.output.format`: GCS Output File Format (one of: avro,parquet,csv,json)(Defaults to csv)
* `pubsublite.to.gcs.timeout`: Time for which the subscription will be read (measured in seconds)
* `pubsublite.to.gcs.processing.time`: Time at which the query will be triggered to process input data (measured in seconds) (format: `"1 second"`)

## Usage

```
$ python main.py --template PUBSUBLITETOGCS --help

usage: main.py --template PUBSUBLITETOGCS [-h] \
	--pubsublite.to.gcs.input.subscription.url PUBSUBLITE.GCS.INPUT.SUBSCRIPTION.URL \
	--pubsublite.to.gcs.output.location PUBSUBLITE.GCS.OUTPUT.LOCATION \
	--pubsublite.to.gcs.checkpoint.location PUBSUBLITE.GCS.CHECKPOINT.LOCATION \
    --pubsublite.to.gcs.timeout PUBSUBLITE.GCS.TIMEOUT \
    --pubsublite.to.gcs.processing.time PUBSUBLITE.GCS.PROCESSING.TIME \

optional arguments:
  -h, --help            show this help message and exit
  --pubsublite.to.gcs.write.mode PUBSUBLITE.TO.GCS.WRITE.MODE 
            {overwrite,append,ignore,errorifexists} Output Write Mode (Defaults to append)
  --pubsublite.to.gcs.output.format PUBSUBLITE.TO.GCS.OUTPUT.FORMAT
            {avro,parquet,csv,json} Output Format (Defaults to csv)
```

## Required JAR files

It uses the [PubSubLite Spark SQL Streaming](https://central.sonatype.com/artifact/com.google.cloud/pubsublite-spark-sql-streaming/1.0.0) for reading data from Pub/Sub lite to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=my-project
export JARS="gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar"
export GCS_STAGING_LOCATION="gs://my-bucket"
export REGION=us-central1
	
./bin/start.sh \
-- --template=PUBSUBLITETOGCS \
    --pubsublite.to.gcs.input.subscription.url=projects/my-project/locations/us-central1/subscriptions/pubsublite-subscription,
    --pubsublite.to.gcs.write.mode=append,
    --pubsublite.to.gcs.output.location=gs://outputLocation,
    --pubsublite.to.gcs.checkpoint.location=gs://checkpointLocation,
    --pubsublite.to.gcs.output.format="csv"
    --pubsublite.to.gcs.timeout=120
    --pubsublite.to.gcs.processing.time="1 second"
```

## PubSubLite to BigQuery


Template for reading files from Cloud Pub/sub Lite and writing them to a BigQuery table.


## Arguments


* `pubsublite.to.bq.input.subscription.url`: Pubsublite input subscription url
* `pubsublite.to.bq.write.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `pubsublite.to.bq.output.dataset`: BigQuery output dataset name
* `pubsublite.to.bq.output.table`: BigQuery output table name
* `pubsublite.to.bq.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector


## Usage


```
$ python main.py --template PUBSUBLITETOBQ --help
usage: main.py --template PUBSUBLITETOBQ [-h] \
   --pubsublite.to.bq.input.subscription.url PUBSUBLITE.BIGQUERY.INPUT.SUBSCRIPTION.URL \
   --pubsublite.to.bq.project.id PUBSUBLITE.BIGQUERY.OUTPUT.PROJECT.ID \
   --pubsublite.to.bq.output.dataset PUBSUBLITE.BIGQUERY.OUTPUT.DATASET \
   --pubsublite.to.bq.output.table PUBSUBLITE.BIGQUERY.OUTPUT.TABLE \
   --pubsublite.to.bq.temp.bucket.name PUBSUBLITE.BIGQUERY.TEMP.BUCKET.NAME \
   [--pubsublite.to.bq.write.mode {overwrite,append,ignore,errorifexists}] \
   --pubsublite.to.bq.checkpoint.location PUBSUBLITE.BIGQUERY.CHECKPOINT.LOCATION

optional arguments:
    -h, --help            show this help message and exit
    --pubsublite.to.bq.processing.time PUBSUBLITE.TO.BQ.PROCESSING.TIME
            (Processing time to write the streaming data into BigQuery)
    --pubsublite.to.bq.input.timeout.sec PUBSUBLITE.TO.BQ.INPUT.TIMEOUT.SEC
            (Stream timeout, for how long the subscription will be read)
```


## Required JAR files


* [Pubsublite Spark SQL Streaming](https://central.sonatype.com/artifact/com.google.cloud/pubsublite-spark-sql-streaming/1.0.0)
* [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example)
It uses the [PubSubLite Spark SQL Streaming](https://central.sonatype.com/artifact/com.google.cloud/pubsublite-spark-sql-streaming/1.0.0) for reading data from Pub/Sub lite to be available in the Dataproc cluster.


## Example submission


```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-name>
export JARS="gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
./bin/start.sh \
-- --template=PUBSUBLITETOBQ \
--pubsublite.to.bq.input.subscription.url=<pubsublite input subscription url (Format: projects/{input_project_id}/locations/{region}/subscriptions/{input_subscription_name})> \
--pubsublite.to.bq.project.id=<BQ project ID> \
--pubsublite.to.bq.output.dataset=<BQ dataset name> \
--pubsublite.to.bq.output.table=<BQ table name> \
--pubsublite.to.bq.write.mode=<Output write mode> \
--pubsublite.to.bq.temp.bucket.name=<temp bucket name> \
--pubsublite.to.bq.checkpoint.location=<checkpoint folder location>
```


## Configurable Parameters


Following properties are available in commandline or [template.constants](../util/template_constants.py) file:


```
## Project that contains the input Pub/Sub subscription to be read
pubsublite.to.bq.project.id=<pubsublite project id>
## PubSublite subscription url
pubsublite.to.bq.input.subscription.url=<pubsublite subscription url (Format: projects/{input_project_id}/locations/{region}/subscriptions/{input_subscription_name})>
## BigQuery output dataset
pubsublite.to.bq.output.dataset=<bq output dataset>
## BigQuery output table
pubsublite.to.bq.output.table=<bq output table>
## BigQuery output write mode
pubsublite.to.bq.write.mode=<bq output write mode>
## Pubsublite to BigQuery temp staging bucket
pubsublite.to.bq.temp.bucket.name=<pubsublite to bq temp staging bucket>
## Pubsublite to BigQuery checkpoint location
pubsublite.to.bq.checkpoint.location=<pubsublite to bq checkpoint location>
## Pubsublite to BigQuery processing time
pubsublite.to.bq.processing.time=<pubsublite to bq processing time>
```
