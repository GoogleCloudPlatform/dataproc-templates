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

#Offset to start reading from. Accepted values: "earliest", "latest" (streaming only), or json string """ {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} """
kafka.bq.starting.offset=<kafka-starting-offset>

#Waits for specified time in ms before termination of stream
kafka.bq.await.termination.timeout=<stream-await-termination-timeout>

#Fails the job when data is lost. Accepted values: true, false
kafka.bq.fail.on.dataloss=<spark-config-fail-on-dataloss>

#Ouptut mode for writing data. Accepted values: 'append', 'complete', 'update'
kafka.bq.steam.output.mode=<output-mode>
```

### Example submission
```
export GCP_PROJECT=<project_id>
export REGION=<region_name>
export SUBNET=<subnet_name> 
export GCS_STAGING_LOCATION=<gs://bucket>
bin/start.sh \
-- \
--template KAFKATOBQ \
--templateProperty project.id=<project_id> \
--templateProperty kafka.bq.checkpoint.location=<gs://bucket/path> \
--templateProperty kafka.bq.bootstrap.servers=<host1:port1,host2:port2> \
--templateProperty kafka.bq.topic=<topic1,topic2> \
--templateProperty kafka.bq.starting.offset=<earliest / latest / """ {"topic1":{"0":10,"1":-1},"topic2":{"0":-2}} """> \
--templateProperty kafka.bq.dataset=<bq_dataset> \
--templateProperty kafka.bq.table=<bq_table> \
--templateProperty kafka.bq.temp.gcs.bucket=<bucket> \
--templateProperty kafka.bq.await.termination.timeout=<timeout_in_ms>
```