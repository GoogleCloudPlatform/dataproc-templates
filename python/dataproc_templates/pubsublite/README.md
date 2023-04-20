# Pub/Sub Lite to GCS

Template for reading files from Pub/Sub Lite and writing them to Google Cloud Storage.


## Arguments

* `pubsublite.to.gcs.input.subscription.url`: PubSubLite Input Subscription Url
* `pubsublite.to.gcs.write.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `pubsublite.to.gcs.output.location`: GCS Location to put Output Files (format: `gs://BUCKET/...`)
* `pubsublite.to.gcs.checkpoint.location`: GCS Checkpoint Folder Location
* `pubsublite.to.gcs.output.format`: GCS Output File Format (one of: avro,parquet,csv,json) (Defaults to json)
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
            {avro,parquet,csv,json} Output Format (Defaults to json)
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
    --pubsublite.to.gcs.input.subscription.url=projects/my-project/locations/us-central1/subscriptions/pubsublite-subscription \
    --pubsublite.to.gcs.write.mode=append \
    --pubsublite.to.gcs.output.location=gs://outputLocation \
    --pubsublite.to.gcs.checkpoint.location=gs://checkpointLocation \
    --pubsublite.to.gcs.output.format="json" \
    --pubsublite.to.gcs.timeout=120 \
    --pubsublite.to.gcs.processing.time="1 second"
```

# Pub/Sub Lite to Bigtable

Template for reading data from Pub/Sub Lite and writing it to Bigtable.

## Arguments

* `pubsublite.bigtable.subscription.path`: Pub/Sub Lite subscription path in the format `projects/<PROJECT>/locations/<REGION>/subscriptions/<SUBSCRIPTION>`
* `pubsublite.bigtable.streaming.timeout` : (Optional) Time duration after which the streaming query will be stopped (measured in seconds). Default: `60`
* `pubsublite.bigtable.streaming.trigger` : (Optional) Time interval at which the streaming query periodically runs to process incoming data (format: string such as `"5 seconds"` or `"3 minutes"`). Default: `"0 seconds"`
* `pubsublite.bigtable.streaming.checkpoint` : (Optional) Temporary folder path to store checkpoint information
* `pubsublite.bigtable.output.project` : GCP project containing the Bigtable instance
* `pubsublite.bigtable.output.instance` : Bigtable instance ID, containing the output table
* `pubsublite.bigtable.output.table` : Table ID in the Bigtable instance, to store the output

**If the table doesn't exist in Bigtable instance, the following arguments can be passed to create one:**

* `pubsublite.bigtable.output.column.families` : List of Column family names to create a new table (format:`"cf1, cf2, cf3"`).
* `pubsublite.bigtable.output.max.versions` : (Optional) Maximum number of versions of cells to keep in the new table (Garbage Collection Policy). Default: `1`

## Usage

```
$ python main.py --template PUBSUBLITETOBIGTABLE --help

usage: main.py [-h] \
--pubsublite.bigtable.subscription.path PUBSUBLITE.BIGTABLE.SUBSCRIPTION.PATH \
[--pubsublite.bigtable.streaming.timeout PUBSUBLITE.BIGTABLE.STREAMING.TIMEOUT] \
[--pubsublite.bigtable.streaming.trigger PUBSUBLITE.BIGTABLE.STREAMING.TRIGGER] \
[--pubsublite.bigtable.streaming.checkpoint.path PUBSUBLITE.BIGTABLE.STREAMING.CHECKPOINT.PATH] \
--pubsublite.bigtable.output.project PUBSUBLITE.BIGTABLE.OUTPUT.PROJECT \
--pubsublite.bigtable.output.instance PUBSUBLITE.BIGTABLE.OUTPUT.INSTANCE \
--pubsublite.bigtable.output.table PUBSUBLITE.BIGTABLE.OUTPUT.TABLE \
[--pubsublite.bigtable.output.column.families PUBSUBLITE.BIGTABLE.OUTPUT.COLUMN.FAMILIES] \
[--pubsublite.bigtable.output.max.versions PUBSUBLITE.BIGTABLE.OUTPUT.MAX.VERSIONS]

optional arguments:
  -h, --help            show this help message and exit
  --pubsublite.bigtable.subscription.path PUBSUBLITE.BIGTABLE.SUBSCRIPTION.PATH
                        Pub/Sub Lite subscription path
  --pubsublite.bigtable.streaming.timeout PUBSUBLITE.BIGTABLE.STREAMING.TIMEOUT
                        Time duration after which the streaming query will be stopped (in seconds)
  --pubsublite.bigtable.streaming.trigger PUBSUBLITE.BIGTABLE.STREAMING.TRIGGER
                        Time interval at which the streaming query runs to process incoming data
  --pubsublite.bigtable.streaming.checkpoint.path PUBSUBLITE.BIGTABLE.STREAMING.CHECKPOINT.PATH
                        Temporary folder path to store checkpoint information
  --pubsublite.bigtable.output.project PUBSUBLITE.BIGTABLE.OUTPUT.PROJECT
                        GCP project containing the Bigtable instance
  --pubsublite.bigtable.output.instance PUBSUBLITE.BIGTABLE.OUTPUT.INSTANCE
                        Bigtable instance ID, containing the output table
  --pubsublite.bigtable.output.table PUBSUBLITE.BIGTABLE.OUTPUT.TABLE
                        Table ID in Bigtable, to store the output
  --pubsublite.bigtable.output.column.families PUBSUBLITE.BIGTABLE.OUTPUT.COLUMN.FAMILIES
                        List of Column Family names to create a new table
  --pubsublite.bigtable.output.max.versions PUBSUBLITE.BIGTABLE.OUTPUT.MAX.VERSIONS
                        Maximum number of versions of cells in the new table (Garbage Collection Policy)

```

## Pub/Sub Lite message format

The input message has to be a `string` in the following format for one rowkey.

```json
{ 
    "rowkey":"rk1",
    "columns": [
        {
            "columnfamily":"place",
            "columnname":"city",
            "columnvalue":"Bangalore"
        },
        {
            "columnfamily":"place",
            "columnname":"state",
            "columnvalue":"Karnataka"
        },
        {
            "columnfamily":"date",
            "columnname":"year",
            "columnvalue":"2023"
        }
    ] 
}
```

The below command can be used as an example which populates a message in the lite topic LT1:

```sh
gcloud pubsub lite-topics publish LT1 \
--location=us-west1 \
--message='''
{ 
    "rowkey":"rk1",
    "columns": [
        {
            "columnfamily":"place",
            "columnname":"city",
            "columnvalue":"Bangalore"
        },
        {
            "columnfamily":"place",
            "columnname":"state",
            "columnvalue":"Karnataka"
        },
        {
            "columnfamily":"date",
            "columnname":"year",
            "columnvalue":"2023"
        },
        {
            "columnfamily":"date",
            "columnname":"month",
            "columnvalue":"March"
        }
    ] 
}
'''
```

## Required JAR files

[Pub/Sub Lite Spark Connector](https://github.com/googleapis/java-pubsublite-spark) is used for reading data from Pub/Sub Lite and needs to be available in the Dataproc cluster.

## Example submission

```sh
export GCP_PROJECT="my-project"
export JARS="gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar"
export GCS_STAGING_LOCATION="gs://my-bucket"
export REGION="us-west1"
export SUBNET="projects/my-project/regions/us-west1/subnetworks/test-subnet"

./bin/start.sh \
-- --template=PUBSUBLITETOBIGTABLE \
--pubsublite.bigtable.subscription.path="projects/$GCP_PROJECT/locations/$REGION/subscriptions/psltobt-sub" \
--pubsublite.bigtable.streaming.checkpoint.location="gs://temp-bucket/checkpoint" \
--pubsublite.bigtable.output.project="my-project" \
--pubsublite.bigtable.output.instance="bt-instance-1" \
--pubsublite.bigtable.output.table="output_table" \
--pubsublite.bigtable.streaming.timeout=20
```

The below [cbt CLI](https://cloud.google.com/bigtable/docs/cbt-overview) command to [read rows](https://cloud.google.com/bigtable/docs/cbt-reference#read_rows) can be used as an example to verify if data was written to the Bigtable table named `output_table` :

```sh
cbt -project my-project -instance bt-instance-1 read output_table
```