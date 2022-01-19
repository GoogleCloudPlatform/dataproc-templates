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
## Project that contains the input PubSub subscription to be read
pubsub.input.project.id=<pubsub project id>
## PubSub subscription name
pubsub.input.subscription=<pubsub subscription>
## Stream timeout, for how long the subscription will be read
pubsub.timeout.ms=60000
## Streaming duration, how often wil writes to BQ be triggered
pubsub.streaming.duration.seconds=15
## Project that contains the output table
pubsub.bq.output.project.id=<pubsub to bq output project id>
## Big Query output dataset
pubsub.bq.output.dataset=<bq output dataset>
## Big Query output table
pubsub.bq.output.table=<bq output table>
```
