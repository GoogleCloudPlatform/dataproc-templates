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

There are two optional properties as well with "Hive to BigQuery" Template. Please find below the details :-

```
--templateProperty hivetobq.temp.table='temporary_view_name' 
--templateProperty hivetobq.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into BigQuery.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"


## 2. Hive To Cloud Storage
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
## Cloud Storage output path.
hive.gcs.output.path=<gcs-output-path>
## Name of hive input table.
hive.input.table=<hive-input-table>
## Hive input db name.
hive.input.db=<hive-output-db>
## Optional - Cloud Storage output format. avro/csv/parquet/json/orc, defaults to avro.
hive.gcs.output.format=avro
## Optional, column to partition hive data.
hive.partition.col=<hive-partition-col>
## Optional: Write mode to Cloud Storage append/overwrite/errorifexists/ignore, defaults to overwrite
hive.gcs.save.mode=overwrite
```

There are two optional properties as well with "Hive to Cloud Storage" Template. Please find below the details :-

```
--templateProperty hive.gcs.temp.table='temporary_view_name' 
--templateProperty hive.gcs.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into Cloud Storage.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"
