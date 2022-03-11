## 1. Pub/Sub To BigQuery

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
--scopes=https://www.googleapis.com/auth/pubsub,https://www.googleapis.com/auth/bigquery \
-- --template PUBSUBTOBIGQUERY
```

### Configurable Parameters
Update Following properties in  [template.properties](../../../../../../../resources/template.properties) file:
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

## 2. Pub/Sub To GCS

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
--scopes=https://www.googleapis.com/auth/pubsub,https://www.googleapis.com/auth/devstorage.read_write \
-- --template PUBSUBTOGCS
```

### Configurable Parameters
Update Following properties in  [template.properties](../../../../../../../resources/template.properties) file:
```
# PubSub to GCS
## Project that contains the input Pub/Sub subscription to be read
pubsubtogcs.input.project.id=<pubsub project id>
## PubSub subscription name
pubsubtogcs.input.subscription=<pubsub subscription>
## Stream timeout, for how long the subscription will be read
pubsubtogcs.timeout.ms=60000
## Streaming duration, how often wil writes to GCS be triggered
pubsubtogcs.streaming.duration.seconds=15
## Number of streams that will read from Pub/Sub subscription in parallel
pubsubtogcs.total.receivers=5
## Project that contains the GCS output
pubsubtogcs.gcs.output.project.id=<pubsub to gcs output project id>
## GCS bucket URL
pubsubtogcs.gcs.bucket.url=<pubsub to gcs output project id>
## Number of records to be written per message to GCS
pubsubtogcs.batch.size=1000
```