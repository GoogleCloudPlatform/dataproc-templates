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


## Executing Redshift to GCS template

General Execution:

```
export GCP_PROJECT=<gcp-project-id> 
export REGION=<region>  
export SUBNET=<subnet>   
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS=gs://<cloud-storage-bucket-name>/spark-redshift_<version>.jar,gs://<cloud-storage-bucket-name>/redshift_jdbc_<version>.jar,gs://<cloud-storage-bucket-name>/minimal_json<version>.jar

bin/start.sh \
-- --template REDSHIFTTOGCS \
--templateProperty project.id=<gcp-project-id> \
--templateProperty redshift.aws.input.url=<jdbc:redshift://host-name:port-number/> \
--templateProperty redshift.aws.input.table=<Redshift-table-name> \
--templateProperty redshift.aws.input.temp.dir=<AWS-temp-directory> \
--templateProperty redshift.aws.input.iam.role=<Redshift-S3-IAM-role> \
--templateProperty redshift.aws.input.access.key=<Access-key> \
--templateProperty redshift.aws.input.secret.key=<Secret-key> \
--templateProperty redshift.gcs.output.file.format=<Output-File-Format> \
--templateProperty redshift.gcs.output.file.location=<Output-GCS-location> \
--templateProperty redshift.gcs.output.mode=<Output-GCS-Save-mode>
```