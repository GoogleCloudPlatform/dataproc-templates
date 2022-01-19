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
--templateProperty hivetobq.bigquery.location=<bigquery destination> \
--templateProperty hivetobq.input.table=<table> \
--templateProperty hivetobq.input.db=<database> \
--templateProperty hivetobq.append.mode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty hivetobq.partition.col=<value> \
--templateProperty hivetobq.spark.sql.warehouse.dir=<gcs-path>
```

### Configurable Parameters
Update Following properties in  [template.properties](../../../../../../../resources/template.properties) file:
```
hivetobq.bigquery.location=<bigquery-location>
hivetobq.input.table=<hive-input-table>
hivetobq.input.db=<hive-input-db>
#Write mode to use while writing output to BQ. Supported values are - Append/Overwrite/ErrorIfExists/Ignore
hivetobq.append.mode=ErrorIfExists
# Optional, column to partition by
hivetobq.partition.col=
# Spark warehouse directory location
hivetobq.spark.sql.warehouse.dir=<spark-warehouse-directory>
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
```

