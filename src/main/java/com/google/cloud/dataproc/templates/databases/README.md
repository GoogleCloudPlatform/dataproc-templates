## Executing Spanner to GCS template

General Execution:

```
GCP_PROJECT=<gcp-project-id>
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template SPANNERTOGCS
```

### Export query results as avro
Update [template.properties](../../../../../../../resources/template.properties) `table.id` property as follows:
```
table.id=(select name, age, phone from employee where designation = 'engineer')
```

**NOTE** It is required to surround your custom query with parenthesis.