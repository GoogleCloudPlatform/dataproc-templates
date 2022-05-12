## Dataplex GCS to BigQuery

This template will incrementally move data from a Dataplex GCS tables to BigQuery. \ 
It will identify new partitions in Dataplex GCS and load them to BigQuery.


### General Execution:

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

`--partitionType` Supported types are: `HOUR`, `DAY`, `MONTH`, `YEAR`

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