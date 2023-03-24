## 1. S3 To BigQuery

### Setup:

Dataproc Servereless requires Cloud NAT in order to have access beyond GCP. 
\
\
To enable this follow [these steps](https://cloud.google.com/nat/docs/using-nat#creating_nat).
In step (4) select the VPC from which you intend to run the Dataproc Serverless job.


### General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_BUCKET=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template S3TOBIGQUERY \
--templateProperty project.id=<project-id> \
--templateProperty s3.bq.access.key=<s3-accesss-key> \
--templateProperty s3.bq.secret.key=<s3-secret-key> \
--templateProperty s3.bq.input.format=<avro,parquet,csv,json> \
--templateProperty s3.bq.input.location=<s3-input-location> \
--templateProperty s3.bq.output.dataset.name=<bq-dataset-name> \
--templateProperty s3.bq.output.table.name=<bq-output-table> \ 
--templateProperty s3.bq.output.mode=<Append|Overwrite|ErrorIfExists|Ignore> \ 
--templateProperty s3.bq.ld.temp.bucket.name=<temp-bucket>
```
**Note**: S3 input location must begin with `s3a://`.

### Configurable Parameters
Optionally update the following properties in the [template.properties](../../../../../../../resources/template.properties) file:
```
project.id=<project-id>
s3.bq.access.key=<s3-accesss-key>
s3.bq.secret.key=<s3-secret-key>
s3.bq.input.format=<avro,parquet,csv,json>
s3.bq.input.location=<s3-input-location>
s3.bq.output.dataset.name=<bq-dataset-name>
s3.bq.output.table.name=<bq-output-table>
s3.bq.output.mode=<Append|Overwrite|ErrorIfExists|Ignore>
s3.bq.ld.temp.bucket.name=<temp-bucket>
```
Note that template properties provided as arguments in the execution command will have priority over those specified in the template.properties file.
