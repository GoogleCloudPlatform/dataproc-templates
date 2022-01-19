## GCS To BigQuery

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template GCSTOBIGQUERY \
--templateProperty project.id=<gcp-project-id> \
--templateProperty gcs.bigquery.input.location=<gcs path> \
--templateProperty gcs.bigquery.input.format=<csv|parquet|avro> \
--templateProperty gcs.bigquery.output.dataset=<datasetId> \
--templateProperty gcs.bigquery.output.table=<tableName> \
--templateProperty gcs.bigquery.temp.bucket.name=<bigquery temp bucket name>
```
