## Dataplex GCS to BigQuery

This template will incrementally move data from a Dataplex GCS tables to BigQuery. \ 
It will identify new partitions in Dataplex GCS and copy them to BigQuery.

### General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template DATAPLEXGCSTOBQ  \
--templateProperty project.id=${PROJECT} \
--templateProperty dataplex.gcs.bq.target.dataset=<dataset_name> \
--templateProperty gcs.bigquery.temp.bucket.name=<temp-bucket-name> \
--dataplexAsset "projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/asset/{asset_id}" \
--customSqlGcsPath "gs://bucket/path/to/custom_sql.sql" 
```

### Specifying Dataplex tables to load
##### 1) Providing asset 
By providing the asset, all tables within that asset will be loaded.\
This is achieved by using the `--dataplexAsset` argument as shown here:
```
--dataplexAsset projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/asset/{asset_id}
```

##### 2) Providing a list of tables (Dataplex Entities)
By providing a list of tables, each table in the list will be loaded.\
This is achieved by providing a string with comma seperated values to the `--dataplexEntityList` as show here:
```
--dataplexEntityList "projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id_1},projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity_id_2}"
```

### Custom SQL 

Optionally a custom SQL can be provided to filter the data that will be copied to BigQuery. \
The template will read from a GCS file with the custom sql string.

The path to this file must be provided with the option `--customSqlGcsPath`. 

Custom SQL must reference `__table__` in the FROM clause as shown in the following example:

```
SELECT 
    col1, col2
FROM
    __table__
WHERE 
    id > 100
```