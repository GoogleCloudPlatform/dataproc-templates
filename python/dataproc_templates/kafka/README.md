# Kafka To BigQuery

Template for reading files from Cloud Storage and writing them to a BigQuery table. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the 
  - [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

  - [Kafka-Clients Connector](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients) for connecting list of borker connections.

## Arguments

* `kafka.to.bq.checkpoint.location`: Cloud Storage location for storing checkpoints during transfer (format: `gs://bucket/...`)
* `kafka.bootstrap.servers`: List of kafka bootstrap servers (format: *'[x1.x2.x3.x4:port1,y1.y2.y3.y4:port2]')*
* `kafka.bq.topic`: Topic names for respective kafka server
* `kafka.to.bq.starting.offset`:  Offset to start reading from. Accepted values: "earliest", "latest" (streaming only), or json string """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
* `kafka.to.bq.dataset`: Temporary bucket for the Spark BigQuery connector
* `kafka.to.bq.table`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `kafka.to.bq.temp.bucket.name`: Name of bucket for temporary storage files (not location).
* `kafka.to.bq.termination.timeout`: **(in seconds)** Waits for specified time in ms before termination of stream 

## Usage

```
$ python main.py --template KAFKATOBQ --help

usage: main.py --template KAFKATOBQ [-h] \
    --kafka.to.bq.checkpoint.location KAFKA.BIGQUERY.CHEKPOINT.LOCATION \
    --kafka.bootstrap.servers KAFKA.BOOTSTRAP.SERVERS \
    --kafka.bq.topic KAFKA.BIGQUERY.TOOPIC \
    --kafka.to.bq.starting.offset {earliest, laterst, json_ strig} \
    --kafka.to.bq.dataset KAFKA.BQ.DATASET \
    --kafka.to.bq.table KAFKA.BQ.TABLE.NAME \
    --kafka.to.bq.temp.bucket.name KAFKA.BIGQUERY.TEMP.BUCKET.NAME

```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example)  and [Kafka-Clients Connector](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients) to be available in the Dataproc cluster.


## Example submission

```
-export GCP_PROJECT=<gcp-project>
-export REGION=<region> 
-export GCS_STAGING_LOCATION=<gcs-staging-location>
-export SUBNET=<subnet>
-export JARS="gs://{jar-bucket}/spark-sql-kafka-0-10_2.12-3.2.0.jar,gs://{jar-bucket}/kafka-clients-2.8.0.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar,gs://{jar-bucket}/commons-pool2-2.6.2.jar,gs://{jar-bucket}/spark-token-provider-kafka-0-10_2.12-3.2.0.jar"

-./bin/start.sh \
--- --template=KAFKATOBQ \
  --log-level=<'ERROR'|'INFO'>
  --kafka.to.bq.checkpoint.location="<gcs checkpoint storage location>" \
   --kafka.bootstrap.servers="<list of kafka connections>" \
   --kafka.bq.topic="<integration topics to subscribe>" \
   --kafka.to.bq.starting.offset="<earliest|latest|json_offset>" \
   --kafka.to.bq.dataset="<bigquery_dataset_name>" \
   --kafka.to.bq.table="<bigquery_table_name>" \
   --kafka.to.bq.temp.bucket.name="<bucket name for staging files>" \
   --kafka.to.bq.termination.timeout="time in seconds"
```

# Kafka To GCS

Template for reading files from Kafka topic and writing them to a GCS bucket. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the Kafka-Clients and Spark-Sql Kafka jars to write streaming data from Kafka topic to GCS .

## Required JAR files

  -  [Kafka Source Structured Streaming](https://mvnrepository.com/artifact/org.apache.spark/spark-streaming_2.13/3.2.0)
  -   [Kafka Clients](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/3.2.0)

   
## Arguments

* `kafka.gcs.checkpoint.location`: Cloud Storage location for storing checkpoints during transfer (format: `gs://bucket/...`)
* `kafka.gcs.output.location.gcs.path`: Output GCS Location for storing streaming data
* `kafka.gcs.bootstrap.servers`: lList of kafka bootstrap servers (format: *'[x1.x2.x3.x4:port1,y1.y2.y3.y4:port2]')*
* `kafka.gcs.topic`: Topic names for respective kafka server
* `kafka.starting.offset`: Offset to start reading from. Accepted values: "earliest", "latest" (streaming only), or json string """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
* `kafka.gcs.output.format`: csv| json| parquet| avro 
* `kafka.gcs.output.mode`: append|overwrite
* `kafka.termination.timeout`: timeout **(in seconds)**

## Usage

```
$ python main.py --template KAFKATOGCS --help                
                        
usage: main.py [-h] --kafka.gcs.checkpoint.location KAFKA.GCS.CHECKPOINT.LOCATION
                    --kafka.gcs.output.location.gcs.path KAFKA.GCS.OUTPUT.PATH
                    --kafka.gcs.bootstrap.servers
                     {KAFKA.GCS.SERVER.IP}
                    --kafka.gcs.topic KAFKA.GCS.TOPIC.NAME
                    --kafka.starting.offset {ealiest,latest,json_checkpoint}
                    --kafka.gcs.output.format {csv, json, avro, parquet}
                    --kafka.gcs.output.mode {append, overwrite}
                    kafka.termination.timeout {timeout in seconds}
```

## Example submission

```
-export GCP_PROJECT=<gcp-project>
-export REGION=<region> 
-export GCS_STAGING_LOCATION=<gcs-staging-location>
-export SUBNET=<subnet>
-export JARS="gs://{jar-bucket}/spark-sql-kafka-0-10_2.12-3.2.0.jar,gs://{jar-bucket}/kafka-clients-2.8.0.jar,gs://{jar-bucket}/commons-pool2-2.6.2.jar,gs://{jar-bucket}/spark-token-provider-kafka-0-10_2.12-3.2.0.jar"

-./bin/start.sh \
--- --template=KAFKATOGCS \
  --log-level=<'ERROR'|'INFO'>
  --kafka.gcs.checkpoint.location="<gcs checkpoint storage location>" \
  --kafka.gcs.output.location.gcs.path= "<gcs output location path>" \
   --kafka.gcs.bootstrap.servers="<list of kafka connections>" \
   --kafka.gcs.topic="<integration topics to subscribe>" \
   --kafka.gcs.starting.offset="<earliest|latest|json_offset>" \
   --kafka.gcs.output.format="{json|csv|avro|parquet}" \
   --kafka.gcs.output.mode="{append|overwrite}" \
   --kafka.gcs.termination.timeout="time in seconds"
```


