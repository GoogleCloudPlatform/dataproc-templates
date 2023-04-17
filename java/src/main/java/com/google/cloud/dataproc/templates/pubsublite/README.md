## 1. Pub/Sub Lite To BigTable

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region> \
SUBNET=<subnet> \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
# ID of Dataproc cluster running permanent history server to access historic logs.
#export HISTORY_SERVER_CLUSTER=<gcp-project-dataproc-history-server-id>


bin/start.sh \
-- --template PUBSUBLITETOBIGTABLE \
--templateProperty pubsub.input.project.id=<pubsub project id> \
--templateProperty pubsub.input.subscription=<pubsub subscription> \
--templateProperty pubsublite.checkpoint.location=<pubsublite checkpoint location> \ 
--templateProperty pubsub.bigtable.output.instance.id=<bigtable instance id> \
--templateProperty pubsub.bigtable.output.project.id=<bigtable output project id> \
--templateProperty pubsub.bigtable.output.table=<bigtable output table>
```

### Configurable Parameters
Following properties are available in commandline or [template.properties](../../../../../../../resources/template.properties) file:

```
## Project that contains the input Pub/Sub subscription to be read
pubsub.input.project.id=<pubsub project id>
## PubSub subscription name
pubsub.input.subscription=<pubsub subscription>
## Stream timeout, for how long the subscription will be read
pubsub.timeout.ms=60000
## Streaming duration, how often wil writes to BQ be triggered
pubsub.streaming.duration.seconds=15
## checkpoint location for the pubsublite topics
pubsublite.checkpoint.location=<checkpoint location>
## Project that contains the output table
pubsub.bigtable.output.project.id=<bigtable output project id>
## BigTable Instance Id
pubsub.bigtable.output.instance.id=<bigtable instance id>
## BigTable output table
pubsub.bigtable.output.table=<bigtable output table>

The input message has to be in the following format for one rowkey.
{
  "rowkey": "rk1",
  "columns": [
    {
      "columnfamily": "cf",
      "columnname": "field1",
      "columnvalue": "value1"
    },
    {
      "columnfamily": "cf",
      "columnname": "field2",
      "columnvalue": "value2"
    }
  ]
}

The below command can be used as an example to populate the message in topic T1:
gcloud pubsub lite-topics publish T1 --message='{"rowkey":"rk1","columns":[{"columnfamily":"cf","columnname":"field1","columnvalue":"value1"},{"columnfamily":"cf","columnname":"field2","columnvalue":"value2"}]}'

Instead if messages are published in other modes, here is the example string to use:
"{ \"rowkey\":\"rk1\",\"columns\": [{\"columnfamily\":\"cf\",\"columnname\":\"field1\",\"columnvalue\":\"value1\"},{\"columnfamily\":\"cf\",\"columnname\":\"field2\",\"columnvalue\":\"value2\"}] }"

(Pleaes note that the table in Bigtable should exist with required column family, before executing the template)
```
