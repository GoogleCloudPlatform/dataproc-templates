## BigQuery To GCS 

General Execution:

```
export GCP_PROJECT=<project_id>
export SUBNET=<region>
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export REGION=<region>

./bin/start.sh \
-- --template=BIGQUERYTOGCS \
	--bigquery.gcs.input.table=<projectId:datasetId.tableName> \
	--bigquery.gcs.output.format=<csv|parquet|avro|json> \
	--bigquery.gcs.output.mode=<overwrite|append|ignore|errorifexists> \
	--bigquery.gcs.output.location=<gs://bucket/path>
```

Note: Since this template uses Spark BigQuery Connector, the shell script appends the JARS variable defined with this dependency to the Dataproc command