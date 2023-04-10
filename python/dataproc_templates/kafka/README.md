# Kafka To GCS

Template for reading files from Kafka topic and writing them to a GCS bucket. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the Spark-Sql Kafka jars to write streaming data from Kafka topic to GCS .

## Required JAR files

  -  [Kafka 0.10+ Source For Structured Streaming](https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.13/3.2.0)

   
## Arguments

* `kafka.gcs.checkpoint.location`: Cloud Storage location for storing checkpoints during transfer (format: `gs://bucket/...`)
* `kafka.gcs.output.location.gcs.path`: Output GCS Location for storing streaming data
* `kafka.gcs.bootstrap.servers`: List of kafka bootstrap servers (format: *'[x1.x2.x3.x4:port1,y1.y2.y3.y4:port2]')*
* `kafka.gcs.topic`: Topic names for respective kafka server
* `kafka.gcs.starting.offset`: Offset to start reading from. Accepted values: "earliest", "latest" (streaming only), or json string """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
* `kafka.gcs.output.format`: csv| json| parquet| avro 
* `kafka.gcs.output.mode`: append|overwrite
* `kafka.gcs.termination.timeout`: timeout **(in seconds)**

## Usage

```
$ python main.py --template KAFKATOGCS --help                
                        
usage: main.py [-h] --kafka.gcs.checkpoint.location KAFKA.GCS.CHECKPOINT.LOCATION
                    --kafka.gcs.output.location.gcs.path KAFKA.GCS.OUTPUT.PATH
                    --kafka.gcs.bootstrap.servers
                     {KAFKA.GCS.SERVER.IP}
                    --kafka.gcs.topic KAFKA.GCS.TOPIC.NAME
                    --kafka.gcs.starting.offset {ealiest,latest,json_checkpoint}
                    --kafka.gcs.output.format {csv, json, avro, parquet}
                    --kafka.gcs.output.mode {append, overwrite}
                    kafka.gcs.termination.timeout {timeout in seconds}
```

## Example submission

```
export GCP_PROJECT=<gcp-project>
export REGION=<region> 
export GCS_STAGING_LOCATION=<gcs-staging-location>
export SUBNET=<subnet>
export JARS="gs://{jar-bucket}/spark-sql-kafka-0-10_2.12-3.2.0.jar,gs://{jar-bucket}/kafka-clients-2.8.0.jar,gs://{jar-bucket}/commons-pool2-2.6.2.jar,gs://{jar-bucket}/spark-token-provider-kafka-0-10_2.12-3.2.0.jar"

./bin/start.sh \
-- --template=KAFKATOGCS \
  --kafka.gcs.checkpoint.location="<gcs checkpoint storage location>" \
  --kafka.gcs.output.location.gcs.path= "<gcs output location path>" \
   --kafka.gcs.bootstrap.servers="<list of kafka connections>" \
   --kafka.gcs.topic="<integration topics to subscribe>" \
   --kafka.gcs.starting.offset="<earliest|latest|json_offset>" \
   --kafka.gcs.output.format="{json|csv|avro|parquet}" \
   --kafka.gcs.output.mode="{append|overwrite}" \
   --kafka.gcs.termination.timeout="time in seconds"
```

# Kafka To BigQuery

Template for reading files from streaming Kafka topic and writing them to a BigQuery table.

It uses the 
  - [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

  - [ Kafka 0.10+ Source For Structured Streaming](https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10) for connecting list of broker connections.

## Arguments

* `kafka.to.bq.checkpoint.location`: Cloud Storage location for storing checkpoints during transfer (format: `gs://bucket/...`)
* `kafka.to.bq.bootstrap.servers`: List of kafka bootstrap servers (format: *'[x1.x2.x3.x4:port1,y1.y2.y3.y4:port2]')*
* `kafka.to.bq.topic`: Topic names for respective kafka server
* `kafka.to.bq.starting.offset`:  Offset to start reading from. Accepted values: "earliest", "latest" (streaming only), or json string """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
* `kafka.to.bq.dataset`: Temporary bucket for the Spark BigQuery connector
* `kafka.to.bq.table`:  name of the bigquery table (destination)
* `kafka.to.bq.output.mode`:  Output mode of the table (append, overwrite, update, complete)
* `kafka.to.bq.temp.bucket.name`: Name of bucket for temporary storage files (not location).
* `kafka.to.bq.termination.timeout`: **(in seconds)** Waits for specified time in sec before termination of stream 


## Usage

```
$ python main.py --template KAFKATOBQ --help

usage: main.py --template KAFKATOBQ [-h] \
    --kafka.to.bq.checkpoint.location KAFKA.BIGQUERY.CHEKPOINT.LOCATION \
    --kafka.to.bq.bootstrap.servers KAFKA.BOOTSTRAP.SERVERS \
    --kafka.to.bq.topic KAFKA.BIGQUERY.TOPIC \
    --kafka.to.bq.starting.offset KAFKA.BIGUERY.STARTING.OFFSET \
    --kafka.to.bq.dataset KAFKA.BQ.DATASET \
    --kafka.to.bq.table KAFKA.BQ.TABLE.NAME \
    --kafka.to.bq.output.mode KAFKA.BQ.OUTPUT.MODE \
    --kafka.to.bq.temp.bucket.name KAFKA.BIGQUERY.TEMP.BUCKET.NAME

```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example)  and [Kafka 0.10+ Source For Structured Streaming](https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10)  to be available in the Dataproc cluster.


## Example submission

```
-export GCP_PROJECT=<gcp-project>
-export REGION=<region> 
-export GCS_STAGING_LOCATION=<gcs-staging-location>
-export SUBNET=<subnet>
-export JARS="gs://{jar-bucket}/spark-sql-kafka-0-10_2.12-3.2.0.jar,gs://{jar-bucket}/kafka-clients-2.8.0.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar,gs://{jar-bucket}/commons-pool2-2.6.2.jar,gs://{jar-bucket}/spark-token-provider-kafka-0-10_2.12-3.2.0.jar"

-./bin/start.sh \
-- --template=KAFKATOBQ \
  --kafka.to.bq.checkpoint.location="<gcs checkpoint storage location>" \
   --kafka.to.bq.bootstrap.servers="<list of kafka connections>" \
   --kafka.to.bq.topic="<integration topics to subscribe>" \
   --kafka.to.bq.starting.offset="<earliest|latest|json_offset>" \
   --kafka.to.bq.dataset="<bigquery_dataset_name>" \
   --kafka.to.bq.table="<bigquery_table_name>" \
   --kafka.to.bq.temp.bucket.name="<bucket name for staging files>" \
   --kafka.to.bq.output.mode=<append|overwrite|update> \
   --kafka.to.bq.termination.timeout="time in seconds"
```

