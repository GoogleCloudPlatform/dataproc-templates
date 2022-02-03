## BigQuery To GCS 

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template BIGQUERYTOGCS \
--templateProperty project.id=<gcp-project-id> \
--templateProperty bigquery.gcs.input.table=<projectId:datasetId.tableName> \
--templateProperty bigquery.gcs.output.format=<csv|parquet|avro|json> \
--templateProperty bigquery.gcs.output.location=<gcs path> \
```
