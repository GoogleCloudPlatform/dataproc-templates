## Executing Spanner to GCS template

General Execution:

```
    bin/start.sh GCP_PROJECT=<gcp-project-id> \
   REGION=<region>  \
   SUBNET=<subnet>   \
   GCS_STAGING_BUCKET=<gcs-staging-bucket-folder> \
   HISTORY_SERVER_CLUSTER=<history-server> \
   TEMPLATE_NAME=SPANNERTOGCS
```

### Export query results as avro
Update [template.properties](../../../../../../../resources/template.properties) `table.id` property as follows:
```
table.id=(select name, age, phone from employee where designation = 'engineer')
```

**NOTE** It is required to surround your custom query with parenthesis.