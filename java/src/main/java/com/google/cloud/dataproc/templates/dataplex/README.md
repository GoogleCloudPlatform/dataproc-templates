## Dataplex Cloud Storage to BigQuery

This template will incrementally move data from a Dataplex Cloud Storage tables to BigQuery.
It will identify new partitions in Dataplex Cloud Storage and load them to BigQuery.

Note: if the Dataplex Cloud Storage table has no partitions, the whole table will be read
from Cloud Storage and the target BQ table will be overwritten.

### General Execution:

Download jar and properties file
```
export GCS_STAGING_LOCATION=gs://bucket/path/to/staging/folder/
gsutil -u <billing-project-id> cp gs://dataplex-dataproc-templates-artifacts/dataproc-templates-1.0-SNAPSHOT.jar ${GCS_STAGING_LOCATION}
gsutil -u <billing-project-id> cp gs://dataplex-dataproc-templates-artifacts/log4j-spark-driver-template.properties ${GCS_STAGING_LOCATION}
```

Create Dataplex Task
```
gcloud dataplex tasks create <task-id> \
    --project=<project-id> \
    --location=<region> \
    --vpc-sub-network-name=<subnet> \
    --lake=<dataplex-lake> \
    --trigger-type=ON_DEMAND \
    --execution-service-account=<execution service account> \
    --spark-main-class="com.google.cloud.dataproc.templates.main.DataProcTemplate" \
    --spark-file-uris="${GCS_STAGING_LOCATION}log4j-spark-driver-template.properties" \
    --container-image-java-jars="${GCS_STAGING_LOCATION}dataproc-templates-1.0-SNAPSHOT.jar" \
    --execution-args=^::^TASK_ARGS="--template=DATAPLEXGCSTOBQ,\
        --templateProperty=project.id=<project-id>,\
        --templateProperty=dataplex.gcs.bq.target.dataset=<dataset_name>,\
        --templateProperty=gcs.bigquery.temp.bucket.name=<temp-bucket-name>,\
        --templateProperty=dataplex.gcs.bq.save.mode=append,\
        --templateProperty=dataplex.gcs.bq.incremental.partition.copy=yes,\
        --dataplexEntity=projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id_1},\
        --partitionField=<partition_field>,\
        --partitionType=<DAY>,\
        --targetTableName=<table_name>,\
        --customSqlGcsPath=<gs://bucket/path/to/custom_sql.sql>" 
```


### Template properties
`project.id` id of the GCP project where the target BigQuery dataset and custom
SQL file should be located

`dataplex.gcs.bq.target.dataset` name of the target BigQuery dataset where the
Dataplex Cloud Storage asset will be migrated to

`gcs.bigquery.temp.bucket.name` the Cloud Storage bucket that temporarily holds the data
before it is loaded to BigQuery

`dataplex.gcs.bq.save.mode` specifies how to handle existing data in BigQuery
if present.
Can be any of the following: `errorifexists`, `append` ,`overwrite`, `ignore`.
Defaults to `errorifexists` \

`dataplex.gcs.bq.incremental.partition.copy` specifies if the template should
copy new partitions only or all the partitions. If set to `no` existing
partitions, if found will be overwritten. Can be any of the following `yes`,
`no`. Defaults to `yes`

`dataplex.gcs.bq.target.asset` specifies the Dataplex BQ Asset where the data will be written to. In other words this 
is an alternative mechanism to specify target dataset. If `dataplex.gcs.bq.target.dataset` and 
`dataplex.gcs.bq.target.asset` are both set, then `dataplex.gcs.bq.target.asset` will take precedence.

`dataplex.gcs.bq.target.entity` specifies tha Dataplex BQ Entity where the data will be written to. In other words this 
is an alternative mechanism to specify the target BQ table. The `dataplex.gcs.bq.target.entity` will take precedence 
over any other property or argument specifying target output of the data.


### Arguments
`--dataplexEntity` Dataplex Cloud Storage table to load in BigQuery \
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
The template will read from a Cloud Storage file with the custom sql string.

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