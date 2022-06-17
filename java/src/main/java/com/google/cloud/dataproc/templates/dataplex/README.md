## Dataplex GCS to BigQuery

This template will incrementally move data from a Dataplex GCS tables to BigQuery.
It will identify new partitions in Dataplex GCS and load them to BigQuery.


### General Execution:
For the execution you can either download or build the project jar.
Both alternatives are explained below.

#### Use precompiled jar
Download jar and properties file
```
export GCS_STAGING_LOCATION=gs://bucket/path/to/staging/folder
gsutil cp gs://dataplex-dataproc-templates-artifacts/dataproc-templates-1.0-SNAPSHOT.jar ${GCS_STAGING_LOCATION}
gsutil cp gs://dataplex-dataproc-templates-artifacts/log4j-spark-driver-template.properties ${GCS_STAGING_LOCATION}
```
Submit the template
```
export JARS=file:///usr/lib/spark/external/spark-avro.jar,${GCS_STAGING_LOCATION}/dataproc-templates-1.0-SNAPSHOT.jar

gcloud beta dataproc batches submit spark \
    --project=<project-id> \
    --region=<region> \
    --jars=${JARS} \
    --labels=job_type=dataproc_template \
    --deps-bucket=${GCS_STAGING_LOCATION} \
    --files=${GCS_STAGING_LOCATION}/log4j-spark-driver-template.properties \
    --class=com.google.cloud.dataproc.templates.main.DataProcTemplate \
    --subnet=<subnet> \
    -- --template=DATAPLEXGCSTOBQ  \
    --templateProperty=project.id=<project-id> \
    --templateProperty=dataplex.gcs.bq.target.dataset=<dataset_name> \
    --templateProperty=gcs.bigquery.temp.bucket.name=<temp-bucket-name> \
    --templateProperty=dataplex.gcs.bq.save.mode="append" \
    --templateProperty=dataplex.gcs.bq.incremental.partition.copy="yes" \
    --dataplexEntity="projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id_1}" \
    --partitionField="partition_field" \
    --partitionType="DAY" \
    --targetTableName="table_name" \
    --customSqlGcsPath="gs://bucket/path/to/custom_sql.sql"
```

#### Build jar and submit template
```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder-path> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template DATAPLEXGCSTOBQ  \
--templateProperty project.id=${PROJECT} \
--templateProperty dataplex.gcs.bq.target.dataset=<dataset_name> \
--templateProperty gcs.bigquery.temp.bucket.name=<temp-bucket-name> \
--templateProperty dataplex.gcs.bq.save.mode="append" \
--templateProperty dataplex.gcs.bq.incremental.partition.copy="yes" \
--dataplexEntity "projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id_1}" \
--partitionField "partition_field" \
--partitionType "DAY" \
--targetTableName "table_name" \
--customSqlGcsPath "gs://bucket/path/to/custom_sql.sql" 
```

### Template properties
`project.id` id of the GCP project where the target BigQuery dataset and custom
SQL file should be located

`dataplex.gcs.bq.target.dataset` name of the target BigQuery dataset where the
Dataplex GCS asset will be migrated to

`gcs.bigquery.temp.bucket.name` the GCS bucket that temporarily holds the data
before it is loaded to BigQuery

`dataplex.gcs.bq.save.mode` specifies how to handle existing data in BigQuery
if present.
Can be any of the following: `errorifexists`, `append` ,`overwrite`, `ignore`.
Defaults to `errorifexists` \

`dataplex.gcs.bq.incremental.partition.copy` specifies if the template should
copy new partitions only or all the partitions. If set to `no` existing
partitions, if found will be overwritten. Can be any of the following `yes`,
`no`. Defaults to `yes`

### Arguments
`--dataplexEntity` Dataplex GCS table to load in BigQuery \
Example: `--dataplexEntityList "projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id_1}"`

`--partitionField` if field is specified together with `partitionType`, the
table is partitioned by this field. The field must be a top-level TIMESTAMP
or DATE field.

`--partitionType` supported types are: `HOUR`, `DAY`, `MONTH`, `YEAR`

`--targetTableName` name of the table where data will be written to. If this
argument is not specified the name of the entity will be used as table name

### Custom SQL

Optionally a custom SQL can be provided to filter the data that will be copied
to BigQuery. \
The template will read from a GCS file with the custom sql string.

The path to this file must be provided with the option `--customSqlGcsPath`.

Custom SQL must reference `__table__` in the FROM clause as shown in the
following example:

```
SELECT 
    col1, col2
FROM
    __table__
WHERE 
    id > 100
```