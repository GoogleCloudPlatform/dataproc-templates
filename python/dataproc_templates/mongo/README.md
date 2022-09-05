## Mongo to GCS

Template for exporting a MongoDB Collection to files in Google Cloud Storage. It supports writing JSON, CSV, Parquet and Avro formats.

It uses the [MongoDB Spark Connector](https://www.mongodb.com/products/spark-connector) and [MongoDB Java Driver](https://jar-download.com/?search_box=mongo-java-driver) for reading data from MongoDB Collections.

## Arguments

* `mongo.gcs.input.uri`: MongoDB Connection String as an Input URI (format: `mongodb://host_name:port_no`)
* `mongo.gcs.input.database`: MongoDB Database Name (format: Database_name)
* `mongo.gcs.input.collection`: MongoDB Input Collection Name (format: Collection_name)
* `mongo.gcs.output.format`: GCS Output File Format (one of: avro,parquet,csv,json)
* `mongo.gcs.output.location`: GCS Location to put Output Files (format: `gs://BUCKET/...`)
* `mongo.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)

## Usage

```
$ python main.py --template MONGOTOGCS --help

usage: main.py --template MONGOTOGCS [-h] \
	--mongo.gcs.input.uri MONGO.GCS.INPUT.URI \
	--mongo.gcs.input.database MONGO.GCS.INPUT.DATABASE \
	--mongo.gcs.input.collection MONGO.GCS.INPUT.COLLECTION \
	--mongo.gcs.output.format {avro,parquet,csv,json} \
	--mongo.gcs.output.location MONGO.GCS.OUTPUT.LOCATION \
    [--mongo.gcs.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --mongo.gcs.input.uri MONGO.GCS.INPUT.URI
                        MongoDB connection URI
  --mongo.gcs.input.database MONGO.GCS.INPUT.DATABASE
                        MongoDB Database Name
  --mongo.gcs.input.collection MONGO.GCS.INPUT.COLLECTION
                        MongoDB Collection Name                      
  --mongo.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --mongo.gcs.output.location MONGO.GCS.OUTPUT.LOCATION
                        GCS location for output files
  --mongo.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
```

## Required JAR files

This template requires the [MongoDB Spark Connector](https://www.mongodb.com/products/spark-connector) and [MongoDB Java Driver](https://jar-download.com/?search_box=mongo-java-driver) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=<project_id>
export JARS="gs://spark-lib/mongodb/mongo-spark-connector_2.12-2.4.0.jar,gs://spark-lib/mongodb/mongo-java-driver-3.9.1.jar"
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export REGION=<region>
	
./bin/start.sh \
-- --template=MONGOTOGCS \
    --mongo.gcs.output.format=<csv|parquet|avro|json> \
    --mongo.gcs.output.location=<gs://bucket/path> \
    --mongo.gcs.output.mode=<overwrite|append|ignore|errorifexists> \
    --mongo.gcs.input.uri=<mongodb://host_name:port_name> \
    --mongo.gcs.input.database=<Database_name> \
    --mongo.gcs.input.collection=<Collection_name>
```
