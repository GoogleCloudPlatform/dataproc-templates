## BigQuery To GCS 

General Execution:

```
export GCP_PROJECT=<gcp-project-id> \
export SUBNET=<region> \
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar" \
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
export REGION=<region> \

./bin/start.sh \
-- --template=BIGQUERYTOGCS \
	--bigquery.gcs.input.table=<projectId:datasetId.tableName> \
	--bigquery.gcs.output.format=<csv|parquet|avro|json> \
	--bigquery.gcs.output.mode=<overwrite|append> \
	--bigquery.gcs.output.location=<gcs-path>
```