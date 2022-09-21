## 1. RedShift To GCS
General Execution:

```
export GCP_PROJECT=<gcp-project-id> 
export REGION=<region>  
export SUBNET=<subnet>   
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS=gs://<cloud-storage-bucket-name>/spark-redshift_<version>.jar

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