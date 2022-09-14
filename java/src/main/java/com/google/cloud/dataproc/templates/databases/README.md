## Executing Spanner to GCS template

General Execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<region>
export SUBNET=<subnet>
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
bin/start.sh \
-- --template SPANNERTOGCS \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty spanner.gcs.input.spanner.id=<spanner-id> \
--templateProperty spanner.gcs.input.database.id=<database-id> \
--templateProperty spanner.gcs.input.table.id=<table-id> \
--templateProperty spanner.gcs.output.gcs.path=<gcs-path> \
--templateProperty spanner.gcs.output.gcs.saveMode=<Append|Overwrite|ErrorIfExists|Ignore>
--templateProperty spanner.gcs.output.gcs.format=<avro|csv|parquet|json|orc>
```

### Export query results as avro
Update`spanner.gcs.input.table.id` property as follows:
```
"spanner.gcs.input.table.id=(select name, age, phone from employee where designation = 'engineer')"
```

**NOTE** It is required to surround your custom query with parenthesis and parameter name with double quotes.

## Executing Cassandra to GCS Template
### General Execution

```
export REGION=<gcp-region>
export GCP_PROJECT=<gcp-project-name>
export GCS_STAGING_LOCATION=<gcs-staging-location>
export JOB_TYPE=SERVERLESS 
export SUBNET=<dataproc-serverless-subnet>

bin/start.sh \
-- --template CASSANDRATOGCS \
--templateProperty cassandratogcs.input.keyspace=<keyspace-name> \
--templateProperty cassandratogcs.input.table=<input-table-name> \
--templateProperty cassandratogcs.input.host=<cassandra-host-ip> \
--templateProperty cassandratogcs.output.format=<avro|csv|parquet|json|orc> \
--templateProperty cassandratogcs.output.savemode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty cassandratogcs.output.path=<gcs-output-path> 
```
### Example Submission:
```
export REGION=us-central1
export GCP_PROJECT=myproject
export GCS_STAGING_LOCATION=gs://staging
export JOB_TYPE=SERVERLESS 
export SUBNET=projects/myproject/regions/us-central1/subnetworks/default

bin/start.sh \
-- --template CASSANDRATOGCS \
--templateProperty cassandratogcs.input.keyspace=testkeyspace \
--templateProperty cassandratogcs.input.table=testtable \
--templateProperty cassandratogcs.input.host=<cassandra-host-ip> \
--templateProperty cassandratogcs.output.format=csv \
--templateProperty cassandratogcs.output.savemode=append \
--templateProperty cassandratogcs.output.path=gs://myproject/cassandraOutput 
```
