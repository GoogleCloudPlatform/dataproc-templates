## BigQuery To Cloud Storage 

General Execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<region>
export SUBNET=<subnet> 
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export HISTORY_SERVER_CLUSTER=<history-server>

bin/start.sh \
-- --template BIGQUERYTOGCS \
--templateProperty project.id=<gcp-project-id> \
--templateProperty bigquery.gcs.input.table=<projectId:datasetId.tableName> \
--templateProperty bigquery.gcs.output.format=<csv|parquet|avro|json> \
--templateProperty bigquery.gcs.output.location=<gcs path> \
--templateProperty bigquery.gcs.output.mode=<Append|Overwrite|ErrorIfExists|Ignore>
```

### Example Submission:
```
export GCP_PROJECT=myproject
export REGION=us-central1
export SUBNET=projects/myproject/regions/us-central1/subnetworks/default
export GCS_STAGING_LOCATION=gs://staging


bin/start.sh \
-- --template BIGQUERYTOGCS \
--templateProperty project.id=myproject \
--templateProperty bigquery.gcs.input.table=myproject:myDataset.empTable \
--templateProperty bigquery.gcs.output.format=csv \
--templateProperty bigquery.gcs.output.location=gs://output/csv \
--templateProperty bigquery.gcs.output.mode=Overwrite
```