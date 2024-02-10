## 1. Kafka To BigQuery

General Execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<gcp-project-region>
export GCS_STAGING_LOCATION=<gcs-bucket-staging-folder-path>
export SUBNET=<gcp-project-dataproc-clusters-subnet>

bin/start.sh \
-- \
--template KAFKATOBQ \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty kafka.bq.checkpoint.location=<gcs-bucket-location-maintains-checkpoint> \
--templateProperty kafka.bq.bootstrap.servers=<kafka-broker-list> \
--templateProperty kafka.bq.topic=<kafka-topic-names> \
--templateProperty kafka.bq.starting.offset=<starting-offset-value> \
--templateProperty kafka.bq.dataset=<output-bigquery-dataset> \
--templateProperty kafka.bq.table=<output-bigquery-table> \
--templateProperty kafka.bq.temp.gcs.bucket=<gcs-bucket-name> \
--templateProperty kafka.bq.await.termination.timeout=<stream-await-termination-timeout>
```

### Configurable Parameters
Following properties are avaialble in commandline or [template.properties](../../../../../../../resources/template.properties) file:

```
# Kafka to BigQuery

# Kafka servers
kafka.bq.bootstrap.servers=<kafka broker list>

# Kafka topics
kafka.bq.topic=<kafka topic names>

# BigQuery output dataset
kafka.bq.dataset=<output bigquery dataset>

# BigQuery output table
kafka.bq.table=<output bigquery table>

# Cloud Storage bucket name, for storing temporary files
kafka.bq.temp.gcs.bucket=<cloud storage bucket name>

# Cloud Storage location for maintaining checkpoint
kafka.bq.checkpoint.location=<cloud storage bucket location maintains checkpoint>

# Offset to start reading from. Accepted values: "earliest", "latest" (streaming only), or json string """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
kafka.bq.starting.offset=<kafka-starting-offset>

# Waits for specified time in ms before termination of stream
kafka.bq.await.termination.timeout=<stream-await-termination-timeout>

# Fails the job when data is lost. Accepted values: true, false
kafka.bq.fail.on.dataloss=<spark-config-fail-on-dataloss>

# Ouptut mode for writing data. Accepted values: 'append', 'complete', 'update'
kafka.bq.stream.output.mode=<output-mode>

# Time in seconds how long data will be collected before dispatching processing on it.
kafka.bq.batch.interval==<kafka.bq.batch.interval>

# The Group ID determines which consumers belong to which group.
kafka.bq.consumer.group.id==<kafka.bq.consumer.group.id>
```

### Important properties

* Usage of `kafka.bq.starting.offset`
    * For batch loads, use earliest, which means start point of the query is set to be the earliest offsets:
        ```
        kafka.bq.starting.offset=earliest
        ```

    * For streaming loads, use latest, which means just start the query from the latest offsets:
        ```
        kafka.bq.starting.offset=latest
        ``` 

    * To read from only specific offsets from a TopicPartition, use a json string in the following format:
        ```
        kafka.bq.starting.offset=""" {"click-events":{"0":15,"1":-1},"msg-events":{"0":-2}} """
        ```
        In the json, -2 as an offset can be used to refer to earliest, -1 to latest.

    Note: The option `kafka.bq.starting.offset` is only relevant when the application is running for the very first time. After that, checkpoint files stored at `kafka.bq.checkpoint.location` are being used.

    To read more this property refer [Structured Streaming + Kafka Integration Guide (Kafka broker version 0.10.0 or higher)](https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html#:~:text=meaning-,startingOffsets,-%22earliest%22%2C%20%22latest%22%20\(streaming)

* Usage of `kafka.bq.stream.output.mode`
    * Append output mode is used when only the new rows in the streaming Dataset needs to be written to the sink.
        ```
        kafka.bq.stream.output.mode=append
        ```
    
    * Complete output mode is used when all the rows in the streaming Dataset needs to be written to the sink every time there are some updates.
        ```
        kafka.bq.stream.output.mode=complete
        ```

    * Update output mode is used when only the rows that were updated in the streaming Dataset needs to be written to the sink every time there are some updates.
        ```
        kafka.bq.stream.output.mode=update
        ```
    For additional details refer the [OutputMode Spark JavaDoc](https://spark.apache.org/docs/2.2.1/api/java/org/apache/spark/sql/streaming/OutputMode.html)

* Usage of `kafka.bq.await.termination.timeout`
    * This property is used to prevent the process from exiting while the query is active. Otherwise, it returns whether the query has terminated or not within the timeoutMs milliseconds.
        ```
        kafka.bq.await.termination.timeout=1800000
        ```
    Note: The default value for this property is 420000


### Example submission
```
export GCP_PROJECT=my-gcp-project
export REGION=us-west1
export SUBNET=test-subnet
export GCS_STAGING_LOCATION=gs://templates-demo-kafkatobq
bin/start.sh \
-- \
--template KAFKATOBQ \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty kafka.bq.checkpoint.location=gs://templates-demo-kafkatobq/checkpoint \
--templateProperty kafka.bq.bootstrap.servers=102.1.1.20:9092 \
--templateProperty kafka.bq.topic=msg-events \
--templateProperty kafka.bq.starting.offset=earliest \
--templateProperty kafka.bq.dataset=kafkatobq \
--templateProperty kafka.bq.table=kafkaevents \
--templateProperty kafka.bq.temp.gcs.bucket=templates-demo-kafkatobq \
--templateProperty kafka.bq.await.termination.timeout=1200000
```


## 2. Kafka To Cloud Storage

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template KAFKATOGCS \
--templateProperty project.id=<gcp-project-id> \
--templateProperty kafka.gcs.output.location=<gcs path> \
--templateProperty kafka.bootstrap.servers=<kafka broker list> \
--templateProperty kafka.topic=<kafka topic name> \
--templateProperty kafka.message.format=<kafka message format> 
```



### Example submission
```
export GCP_PROJECT=dp-test-project
export REGION=us-central1
export SUBNET=test-subnet
export GCS_STAGING_LOCATION=gs://dp-templates-kakfatogcs/stg
export GCS_SCHEMA_FILE=gs://dp-templates-kafkatogcs/schema/msg_schema.json
export GCS_OUTPUT_PATH=gs://dp-templates-kafkatogcs/output/
bin/start.sh \
-- --template KAFKATOGCS \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty kafka.bootstrap.servers=102.1.1.20:9092 \
--templateProperty kafka.topic=events-topic \
--templateProperty kafka.starting.offset=latest \
--templateProperty kafka.message.format=json \
--templateProperty kafka.schema.url=$GCS_SCHEMA_FILE \
--templateProperty kafka.gcs.await.termination.timeout.ms=1200000 \
--templateProperty kafka.gcs.output.location=$GCS_OUTPUT_PATH \
--templateProperty kafka.gcs.output.format=parquet
```

## 3. Kafka To PubSub

General Execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<gcp-project-region>
export GCS_STAGING_LOCATION=<gcs-bucket-staging-folder-path>
export SUBNET=<gcp-project-dataproc-clusters-subnet>

bin/start.sh \
-- \
--template KAFKATOPUBSUB \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty kafka.pubsub.checkpoint.location=<gcs-bucket-location-maintains-checkpoint> \
--templateProperty kafka.pubsub.bootstrap.servers=<kafka-broker-list> \
--templateProperty kafka.pubsub.input.topic=<kafka-topic> \
--templateProperty kafka.pubsub.output.topic=<pubsub-topic> \
--templateProperty kafka.pubsub.output.projectId=<pubsub-project-id> \
--templateProperty kafka.pubsub.starting.offset=<starting-offset-value> \
--templateProperty kafka.pubsub.await.termination.timeout=<stream-await-termination-timeout>
```

### Configurable Parameters
Following properties are avaialble in commandline or [template.properties](../../../../../../../resources/template.properties) file:

```
# Kafka to PubSub

# Kafka bootstrap servers
kafka.pubsub.bootstrap.servers=

# Kafka topics
kafka.pubsub.input.topic=

# PubSub topic
kafka.pubsub.output.topic=

# Cloud Storage location for maintaining checkpoint
kafka.pubsub.checkpoint.location=

# Offset to start reading from. Accepted values: "earliest", "latest" (streaming only), or json string """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
kafka.pubsub.starting.offset=

# Waits for specified time in ms before termination of stream
kafka.pubsub.await.termination.timeout=
```


### Example submission
```
export GCP_PROJECT=my-gcp-project
export REGION=us-west1
export SUBNET=test-subnet
export GCS_STAGING_LOCATION=gs://templates-demo-kafkatobq

bin/start.sh \
-- \
--template KAFKATOPUBSUB \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty kafka.pubsub.checkpoint.location=gs://templates-demo-kafkatopubsub/checkpoint \
--templateProperty kafka.pubsub.bootstrap.servers=102.1.1.20:9092 \
--templateProperty kafka.pubsub.input.topic=msg-events \
--templateProperty kafka.pubsub.output.topic=ps-msg-events \
--templateProperty kafka.pubsub.output.projectId=$GCP_PROJECT \
--templateProperty kafka.pubsub.starting.offset=earliest \
--templateProperty kafka.pubsub.await.termination.timeout=120000
```

## 4. Kafka To BQ via spark Direct stream

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template KafkaToBQDstream \
--templateProperty project.id=<gcp-project-id> \
--templateProperty kafka.bootstrap.servers=<kafka broker list> \
--templateProperty kafka.topic=<kafka topic name> \
--templateProperty kafka.starting.offset=<latest | earliest> \
--templateProperty kafka.bq.stream.output.mode=<Append | Overwrite | ErrorIfExists | Ignore> \
--templateProperty kafka.gcs.batch.interval=<Batch interval of the stream> \
--templateProperty kafka.gcs.consumer.group.id=<Consumer group id for the kafka topic> \
--templateProperty kafka.bq.dataset=kafkatobq \
--templateProperty kafka.bq.table=kafkaevents \
--templateProperty kafka.bq.temp.gcs.bucket=<gcs-bucket-name> \
--templateProperty kafka.bq.await.termination.timeout=<stream-await-termination-timeout>
```

### Configurable Parameters
Following properties are avaialble in commandline or [template.properties](../../../../../../../resources/template.properties) file:

```
# Kafka to BigQuery via Dstream

# Kafka servers
kafka.bootstrap.servers=<kafka broker list>

# Kafka topics
kafka.topic=<kafka topic names>

# BigQuery output dataset
kafka.bq.dataset=<output bigquery dataset>

# BigQuery output table
kafka.bq.table=<output bigquery table>

# Cloud Storage bucket name, for storing temporary files
kafka.bq.temp.gcs.bucket=<cloud storage bucket name>

# Offset to start reading from. Accepted values: "earliest", "latest" 
kafka.bq.starting.offset=<kafka-starting-offset>

# Waits for specified time in ms before termination of stream
kafka.bq.await.termination.timeout=<stream-await-termination-timeout>

# Ouptut mode for writing data. Accepted values: 'overwrite', 'append', 'ignore', 'error', 'errorifexists', 'default'
kafka.bq.stream.output.mode=<output-mode> default value : append
```

### Example submission
```
export GCP_PROJECT=dp-test-project
export REGION=us-central1
export SUBNET=test-subnet
export GCS_STAGING_LOCATION=gs://dp-templates-kakfatogcs/stg
export GCS_OUTPUT_PATH=gs://dp-templates-kafkatogcs/output/
bin/start.sh \
-- --template KafkaToBQDstream \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty kafka.bootstrap.servers=102.1.1.20:9092 \
--templateProperty kafka.topic=events-topic \
--templateProperty kafka.starting.offset=latest \
--templateProperty kafka.bq.stream.output.mode=Append \
--templateProperty kafka.bq.batch.interval=60000 \
--templateProperty kafka.bq.consumer.group.id=test.group.id \
--templateProperty kafka.bq.dataset=kafkatobq \
--templateProperty kafka.bq.table=kafkaevents \
--templateProperty kafka.bq.temp.gcs.bucket=templates-demo-kafkatobq-stream \
--templateProperty kafka.bq.await.termination.timeout=12000
```


## 5. Kafka To GCS via spark Direct stream

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template KafkaTOGCSDstream \
--templateProperty project.id=<gcp-project-id> \
--templateProperty kafka.gcs.output.location=<gcs path> \
--templateProperty kafka.bootstrap.servers=<kafka broker list> \
--templateProperty kafka.topic=<kafka topic name> \
--templateProperty kafka.starting.offset=<latest | earliest> \
--templateProperty kafka.message.format=<bytes | json> 
--templateProperty kafka.gcs.write.mode=<Append | Overwrite | ErrorIfExists | Ignore> \
--templateProperty kafka.gcs.batch.interval=<Batch interval of the stream> \
--templateProperty kafka.gcs.consumer.group.id=<Consumer group id for the kafka topic> \
--templateProperty kafka.gcs.output.format=<json | parquet> \
--templateProperty kafka.schema.url=<json schema url>
```

### Configurable Parameters
Following properties are avaialble in commandline or [template.properties](../../../../../../../resources/template.properties) file:

```
# Kafka to GCS via Dstream

# Kafka servers
kafka.bootstrap.servers=<kafka broker list>

# Kafka topics
kafka.topic=<kafka topic names>

# Offset to start reading from. Accepted values: "earliest", "latest" 
kafka.starting.offset=<kafka-starting-offset>

# Time in seconds how long data will be collected before dispatching processing on it.
kafka.gcs.batch.interval==<kafka.gcs.batch.interval>

# Ouptut mode for writing data. Accepted values: 'overwrite', 'append', 'ignore', 'error', 'errorifexists', 'default'
kafka.gcs.write.mode=<output-mode> default value : append

# Schema url is required if 'kafka.message.format' is set to json
kafka.schema.url==<kafka.schema.url>
```


### Example submission
```
export GCP_PROJECT=dp-test-project
export REGION=us-central1
export SUBNET=test-subnet
export GCS_STAGING_LOCATION=gs://dp-templates-kakfatogcs/stg
export GCS_OUTPUT_PATH=gs://dp-templates-kafkatogcs/output/
bin/start.sh \
-- --template KafkaTOGCSDstream \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty kafka.bootstrap.servers=102.1.1.20:9092 \
--templateProperty kafka.topic=events-topic \
--templateProperty kafka.starting.offset=latest \
--templateProperty kafka.message.format=bytes \
--templateProperty kafka.gcs.output.location=$GCS_OUTPUT_PATH \
--templateProperty kafka.gcs.output.format=parquet \
--templateProperty kafka.gcs.write.mode=Append \
--templateProperty kafka.gcs.batch.interval=60000 \
--templateProperty kafka.gcs.consumer.group.id=test.group.id \
--templateProperty kafka.schema.url=gs://dp-templates-kafkatogcs/schema/msg_schema.json
```