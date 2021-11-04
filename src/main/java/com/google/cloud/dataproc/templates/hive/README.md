## 1. Hive To BigQuery

General Execution:

```
bin/start.sh GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_BUCKET=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
TEMPLATE_NAME=HIVETOBIGQUERY \
--properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083
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
bin/start.sh GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_BUCKET=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
TEMPLATE_NAME=HIVETOGCS \
--properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083
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

