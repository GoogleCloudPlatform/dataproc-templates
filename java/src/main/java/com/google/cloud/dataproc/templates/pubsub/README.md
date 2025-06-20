## 1. Pub/Sub To BigQuery

General Execution:

```shell
GCP_PROJECT=<gcp-project-id> \
REGION=<region> \
SUBNET=<subnet> \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
# ID of Dataproc cluster running permanent history server to access historic logs.
#export HISTORY_SERVER_CLUSTER=<gcp-project-dataproc-history-server-id>


bin/start.sh \
-- --template PUBSUBTOBQ \
--templateProperty pubsub.input.project.id=<pubsub project id> \
--templateProperty pubsub.input.subscription=<pubsub subscription> \
--templateProperty pubsub.bq.output.project.id=<bq output project id> \
--templateProperty pubsub.bq.output.dataset=<bq output dataset> \
--templateProperty pubsub.bq.output.table=<bq output table>
```

### Configurable Parameters
The following properties are available in commandline or [template.properties](../../../../../../../resources/template.properties) file:

```
## Project that contains the input Pub/Sub subscription to be read
pubsub.input.project.id=<pubsub project id>
## PubSub subscription name
pubsub.input.subscription=<pubsub subscription>
## Stream timeout, for how long the subscription will be read
pubsub.timeout.ms=60000
## Streaming duration, how often wil writes to BQ be triggered
pubsub.streaming.duration.seconds=15
## Number of streams that will read from Pub/Sub subscription in parallel
pubsub.total.receivers=5
## Project that contains the output table
pubsub.bq.output.project.id=<pubsub to bq output project id>
## BigQuery output dataset
pubsub.bq.output.dataset=<bq output dataset>
## BigQuery output table
pubsub.bq.output.table=<bq output table>
## Number of records to be written per message to BigQuery
pubsub.bq.batch.size=1000
```
## 2. Pub/Sub To Cloud Storage

General Execution:

```shell
export PROJECT=<gcp-project-id>
export GCP_PROJECT=<gcp-project-id>
export REGION=<gcp-project-region>
export GCS_STAGING_LOCATION=<gcs-bucket-staging-folder-path>
# Set optional environment variables.
export SUBNET=<gcp-project-dataproc-clusters-subnet>
# ID of Dataproc cluster running permanent history server to access historic logs.
#export HISTORY_SERVER_CLUSTER=<gcp-project-dataproc-history-server-id>

# The submit spark options must be seperated with a "--" from the template options
bin/start.sh \
-- \
--template PUBSUBTOGCS \
--templateProperty pubsubtogcs.input.project.id=$GCP_PROJECT \
--templateProperty pubsubtogcs.input.subscription=<pubsub-topic-subscription-name> \
--templateProperty pubsubtogcs.gcs.bucket.name=<gcs-bucket-name> \
--templateProperty pubsubtogcs.gcs.output.data.format=avro or json \
```

### Configurable Parameters
The following properties are available in commandline or [template.properties](../../../../../../../resources/template.properties) file:

```
# PubSub to Cloud Storage
## Project that contains the input Pub/Sub subscription to be read
pubsubtogcs.input.project.id=yadavaja-sandbox
## PubSub subscription name
pubsubtogcs.input.subscription=
## Stream timeout, for how long the subscription will be read
pubsubtogcs.timeout.ms=60000
## Streaming duration, how often wil writes to Cloud Storage be triggered
pubsubtogcs.streaming.duration.seconds=15
## Number of streams that will read from Pub/Sub subscription in parallel
pubsubtogcs.total.receivers=5
## Cloud Storage bucket URL : gs://BUCKET_NAME/path/
pubsubtogcs.gcs.bucket.name=
## Number of records to be written per message to Cloud Storage
pubsubtogcs.batch.size=1000
## PubSub to Cloud Storage supported formats are: avro, json
pubsubtogcs.gcs.output.data.format=
```
## 3. Pub/Sub To BigTable

General Execution:

```shell
GCP_PROJECT=<gcp-project-id> \
REGION=<region> \
SUBNET=<subnet> \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
# ID of Dataproc cluster running permanent history server to access historic logs.
#export HISTORY_SERVER_CLUSTER=<gcp-project-dataproc-history-server-id>


bin/start.sh \
-- --template PUBSUBTOBIGTABLE \
--templateProperty pubsub.input.project.id=<pubsub project id> \
--templateProperty pubsub.input.subscription=<pubsub subscription> \
--templateProperty pubsub.bigtable.output.instance.id=<bigtable instance id> \
--templateProperty pubsub.bigtable.output.project.id=<bigtable output project id> \
--templateProperty pubsub.bigtable.output.table=<bigtable output table> \
--templateProperty pubsub.bigtable.catalog.location=<bigtable catalog location>
```

### Configurable Parameters
The following properties are available in commandline or [template.properties](../../../../../../../resources/template.properties) file:

```
## Project that contains the input Pub/Sub subscription to be read
pubsub.input.project.id=<pubsub project id>
## PubSub subscription name
pubsub.input.subscription=<pubsub subscription>
## Stream timeout, for how long the subscription will be read
pubsub.timeout.ms=60000
## Streaming duration, how often wil writes to BQ be triggered
pubsub.streaming.duration.seconds=15
## Number of streams that will read from Pub/Sub subscription in parallel
pubsub.total.receivers=5
## Project that contains the output table
pubsub.bigtable.output.project.id=<bigtable output project id>
## BigTable Instance Id
pubsub.bigtable.output.instance.id=<bigtable instance id>
## BigTable output table
pubsub.bigtable.output.table=<bigtable output table>
## BigTable table catalog
pubsub.bigtable.catalog.location=<bigtable catalog location>
```
Please refer our public [documentation](https://cloud.google.com/bigtable/docs/use-bigtable-spark-connector) for more details around spark bigtable connector.

The input message has to be in the following format for one rowkey.

```json
{
  "table": {"name": "employee"},
  "rowkey": "id_rowkey",
  "columns": {
    "key": {"cf": "rowkey", "col": "id_rowkey", "type": "string"},
    "name": {"cf": "personal", "col": "name", "type": "string"},
    "address": {"cf": "personal", "col": "address", "type": "string"},
    "empno": {"cf": "professional", "col": "empno", "type": "string"}
  }
}
```
```
(Pleaes note that the table in Bigtable should exist with required column family, before executing the template)
```
We have provided robust logging messages. Spark Streaming application will run on various dataproc workers nodes. Usually Spark application prints messages on a driver as well as executors.
It is tough to check logs directly on the driver side for troubleshooting. Please use below sample Cloud Logging query which will print all messages from the driver and executors.
```
resource.type="cloud_dataproc_batch"
resource.labels.project_id="YOUR_PROJECT_ID"
resource.labels.location="DATAPROC_SERVERLESS_JOB_REGION"
resource.labels.batch_id="JOB_BATCH_ID"
timestamp>="START_TIMESTAMP"
timestamp<="END_TIMESTAMP"
severity>=DEFAULT
```
