## GCS To BigQuery

General Execution:

```
export GCP_PROJECT=<project_id> \
export SUBNET=<region> \
export JARS="file:///usr/lib/spark/external/spark-avro.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar" \
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
export REGION=<region> \

./bin/start.sh \
-- --template=GCSTOBIGQUERY \
    --gcs.bigquery.input.format="<csv|parquet|avro>" \
    --gcs.bigquery.input.location="<gs://bucket/path>" \
    --gcs.bigquery.output.dataset="<dataset>" \
    --gcs.bigquery.output.table="<table>" \
    --gcs.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```