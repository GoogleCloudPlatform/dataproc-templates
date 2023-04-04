## PubSubLite to GCS

Template for reading files from Pub/Sub Lite and writing them to Google Cloud Storage. It supports writing JSON, Parquet and Avro formats.


## Arguments

* `pubsublite.to.gcs.input.subscription.url`: PubSubLite Input Subscription Url
* `pubsublite.to.gcs.write.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `pubsublite.to.gcs.output.location`: GCS Location to put Output Files (format: `gs://BUCKET/...`)
* `pubsublite.to.gcs.checkpoint.location`: GCS Checkpoint Folder Location
* `pubsublite.to.gcs.output.format`: GCS Output File Format (one of: avro,parquet,csv,json)(Defaults to json)
* `pubsublite.to.gcs.timeout.ms`: Time for which the subscription will be read (measured in milliseconds)
* `pubsublite.to.gcs.processing.time.seconds`: Time at which the query will be triggered to process input data (measured in seconds)

## Usage

```
$ python main.py --template PUBSUBLITETOGCS --help

usage: main.py --template PUBSUBLITETOGCS [-h] \
	--pubsublite.to.gcs.input.subscription.url PUBSUBLITE.GCS.INPUT.SUBSCRIPTION.URL \
	--pubsublite.to.gcs.output.location PUBSUBLITE.GCS.OUTPUT.LOCATION \
	--pubsublite.to.gcs.checkpoint.location PUBSUBLITE.GCS.CHECKPOINT.LOCATION \
    --pubsublite.to.gcs.timeout.ms PUBSUBLITE.GCS.TIMEOUT.MS \
    --pubsublite.to.gcs.processing.time.seconds PUBSUBLITE.GCS.PROCESSING.TIME.SECONDS \

optional arguments:
  -h, --help            show this help message and exit
  --pubsublite.to.gcs.write.mode PUBSUBLITE.TO.GCS.WRITE.MODE 
            {overwrite,append,ignore,errorifexists} Output Write Mode (Defaults to append)
  --pubsublite.to.gcs.output.format PUBSUBLITE.TO.GCS.OUTPUT.FORMAT
            {avro,parquet,csv,json} Output Format (Defaults to json)
```

## Required JAR files

It uses the [PubSubLite Spark Sql Streaming](https://repo1.maven.org/maven2/com/google/cloud/pubsublite-spark-sql-streaming/1.0.0/) and [Google Cloud Pubsublite](https://repo1.maven.org/maven2/com/google/cloud/google-cloud-pubsublite/1.9.3/) for reading data from Pub/Sub lite to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=my-project
export GCS_STAGING_LOCATION="gs://my-bucket"
export REGION=us-central1
	
./bin/start.sh \
-- --template=PUBSUBLITETOGCS \
    --pubsublite.to.gcs.input.subscription.url=projects/my-project/locations/us-central1/subscriptions/pubsublite-subscription,
    --templateProperty pubsublite.to.gcs.write.mode=append,
    --templateProperty pubsublite.to.gcs.output.location=gs://outputLocation,
    --templateProperty pubsublite.to.gcs.checkpoint.location=gs://checkpointLocation,
    --templateProperty pubsublite.to.gcs.output.format="json"
    --templateProperty pubsublite.to.gcs.timeout=120000
    --templateProperty pubsublite.to.gcs.processing.time=1
```
