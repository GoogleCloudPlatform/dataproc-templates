## 1. RedShift To GCS
General Execution:

```
export GCP_PROJECT=<gcp-project-id> 
export REGION=<region>  
export SUBNET=<subnet>   
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 

bin/start.sh \
-- --template REDSHIFTTOGCS \
--templateProperty project.id=<gcp-project-id> \
--templateProperty redshift.gcs.input.url=<jdbc:redshift://host-name:port-number/> \
--templateProperty redshift.gcs.input.table=<Redshift-table-name> \
--templateProperty redshift.gcs.temp.dir=<AWS-temp-directory> \
--templateProperty redshift.gcs.iam.role=<Redshift-S3-IAM-role> \
--templateProperty redshift.gcs.access.key=<Access-key> \
--templateProperty redshift.gcs.secret.key=<Secret-key> \
--templateProperty redshift.gcs.file.format=<Output-File-Format> \
--templateProperty redshift.gcs.file.location=<Output-GCS-location> \
--templateProperty redshift.gcs.output.mode=<Output-GCS-Save-mode>
```