## 1. Hive To BigQuery

General Execution:

```
export GCP_PROJECT=<gcp-project-id> \
export REGION=<region>  \
export SUBNET=<subnet>   \
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
export CATALOG=<catalog of hbase table>
 \
bin/start.sh \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=src/main/resources/hbase-site.xml'  \
-- --template HBASETOGCS \
--templateProperty hbasetogcs.fileformat=<avro|csv|parquet|json|orc>  \
--templateProperty hbasetogcs.savemode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty hbasetogcs.output.path=<output-gcs-path>
--templateProperty hbasetogcs.table.catalog=<Hbase Table Catalog>
```
Example catalog -:
```{"table":{"namespace":"default", "name":"my_table"},"rowkey":"key","columns":{"key":{"cf":"rowkey", "col":"key", "type":"string"},"name":{"cf":"cf", "col":"name", "type":"string"}}}```