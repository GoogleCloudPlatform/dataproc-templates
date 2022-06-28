## 1. Hive To BigQuery

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
--properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083 \
-- --template HIVETOBIGQUERY \
--templateProperty hivetobq.bigquery.location=<required_bigquery destination> \
--templateProperty hivetobq.sql=<hive_sql> \
--templateProperty hivetobq.write.mode=<Append|Overwrite|ErrorIfExists|Ignore> \ 
--templateProperty hivetobq.temp.gcs.bucket=<gcs_bucket_path>
```

Have SQL query within double quotes. Example,

```
--templateProperty  hivetobq.sql="select * from dbname.tablename"
```


## 2. Hive To GCS
General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
--properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083 \
-- --template HIVETOGCS \
--templateProperty hive.input.table=<table> \
--templateProperty hive.input.db=<database> \
--templateProperty hive.gcs.output.path=<gcs-output-path>
```

### Configurable Parameters
Update Following properties in [template.properties](../../../../../../../resources/template.properties) file:
```
## Source Hive warehouse dir.
spark.sql.warehouse.dir=<warehouse-path>
## GCS output path.
hive.gcs.output.path=<gcs-output-path>
## Name of hive input table.
hive.input.table=<hive-input-table>
## Hive input db name.
hive.input.db=<hive-output-db>
## GCS output format. Optional, defaults to avro.
hive.gcs.output.format=<gcs-output-format>
## Optional, column to partition hive data.
hive.partition.col=<hive-partition-col>
## Optional: Write mode to gcs append/overwrite/errorifexists/ignore, defaults to overwrite
hive.gcs.save.mode=overwrite
```

