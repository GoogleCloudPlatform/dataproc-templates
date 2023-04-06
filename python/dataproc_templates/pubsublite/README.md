## Pub/Sub Lite to Cloud Storage

Template for reading files from Pub/Sub Lite and writing them to Google Cloud Storage. It supports writing JSON, CSV, Parquet and Avro formats.


## Arguments

* `pubsublite.to.gcs.input.subscription.url`: Pub/Sub Lite Input Subscription Url
* `pubsublite.to.gcs.write.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `pubsublite.to.gcs.output.location`: Cloud Storage Location to put Output Files (format: `gs://BUCKET/...`)
* `pubsublite.to.gcs.checkpoint.location`: Cloud Storage Checkpoint Folder Location
* `pubsublite.to.gcs.output.format`: Cloud Storage Output File Format (one of: avro,parquet,csv,json)(Defaults to csv)
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

It uses the [Pub/Sub Lite Spark SQL Streaming](https://central.sonatype.com/artifact/com.google.cloud/pubsublite-spark-sql-streaming/1.0.0) for reading data from Pub/Sub lite to be available in the Dataproc cluster.

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
