## 1. GCS To BigQuery

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
--templateProperty gcs.bigquery.input.format=<csv|parquet|avro|orc> \
--templateProperty gcs.bigquery.output.dataset=<datasetId> \
--templateProperty gcs.bigquery.output.table=<tableName> \
--templateProperty gcs.bigquery.temp.bucket.name=<bigquery temp bucket name>
```

## 2. GCS to Spanner
```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
bin/start.sh \
-- --template GCSTOSPANNER \
--templateProperty project.id=<gcp-project-id> \
--templateProperty gcs.spanner.input.format=<avro | parquet | orc> \
--templateProperty gcs.spanner.input.location=<gcs path> \
--templateProperty gcs.spanner.output.instance=<spanner instance id> \
--templateProperty gcs.spanner.output.database=<spanner database id> \
--templateProperty gcs.spanner.output.table=<spanner table id> \
--templateProperty gcs.spanner.output.saveMode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty gcs.spanner.output.primaryKey=<column[(,column)*] - primary key columns needed when creating the table>
```


## 3. GCS to JDBC
```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
bin/start.sh \
-- --template GCSTOJDBC \
--templateProperty project.id=<gcp-project-id> \
--templateProperty gcs.jdbc.input.format=<avro | parquet | orc> \
--templateProperty gcs.jdbc.input.location=<gcs path> \
--templateProperty gcs.jdbc.output.url=<jdbc url along with username and password in single quotes> \
--templateProperty gcs.jdbc.output.table=<jdbc connection table id> \
--templateProperty gcs.jdbc.output.saveMode=<Append|Overwrite|ErrorIfExists|Ignore>
```