## 1. GCS To BigQuery

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template GCSTOBIGQUERY \
--templateProperty project.id=<gcp-project-id> \
--templateProperty gcs.bigquery.input.location=<gcs path> \
--templateProperty gcs.bigquery.input.format=<csv|parquet|avro|orc> \
--templateProperty gcs.bigquery.output.dataset=<datasetId> \
--templateProperty gcs.bigquery.output.table=<tableName> \
--templateProperty gcs.bigquery.temp.bucket.name=<bigquery temp bucket name>
```

There are two optional properties as well with "GCS to BigQuery" Template. Please find below the details :-

```
--templateProperty gcs.bigquery.temp.table='temporary_view_name' 
--templateProperty gcs.bigquery.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations while loading data into BigQuery.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

## 2. GCS To BigTable

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template GCSTOBIGTABLE \
--templateProperty project.id=<gcp-project-id> \
--templateProperty gcs.bigtable.input.location=<gcs file location> \
--templateProperty gcs.bigtable.input.format=<csv|parquet|avro> \
--templateProperty gcs.bigtable.output.instance.id=<bigtable instance Id> \
--templateProperty gcs.bigtable.output.project.id=<bigtable project Id> \
--templateProperty gcs.bigtable.table.name=<bigtable tableName> \
--templateProperty gcs.bigtable.column.family=<bigtable column family>

Example execution:-

bin/start.sh \
-- --template GCSTOBIGTABLE \
--templateProperty project.id=my-project-id \
--templateProperty gcs.bigtable.input.location=gs://my-gcp-project-input-bucket/filename.csv \
--templateProperty gcs.bigtable.input.format=csv \
--templateProperty gcs.bigtable.output.instance.id=my-instance-id \
--templateProperty gcs.bigtable.output.project.id=my-project-id \
--templateProperty gcs.bigtable.table.name=my-bt-table \
--templateProperty gcs.bigtable.table.column.family=cf

(Please note that the table in Bigtable should exist with the above given column family before executing the template)
```

## 3. GCS to Spanner
```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
bin/start.sh \
-- --template GCSTOSPANNER \
--templateProperty project.id=<gcp-project-id> \
--templateProperty gcs.spanner.input.format=<avro | parquet | orc> \
--templateProperty gcs.spanner.input.location=<gcs path> \
--templateProperty gcs.spanner.output.instance=<spanner instance id> \
--templateProperty gcs.spanner.output.database=<spanner database id> \
--templateProperty gcs.spanner.output.table=<spanner table id> \
--templateProperty gcs.spanner.output.saveMode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty gcs.spanner.output.primaryKey=<column[(,column)*] - primary key columns needed when creating the table> \
--templateProperty gcs.spanner.output.batchInsertSize=<optional integer>
```


## 4. GCS to JDBC

```
Please download the JDBC Driver of respective database and copy it to gcs bucket location.

export JARS=<gcs location for jdbc connector jar>

GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
bin/start.sh \
-- --template GCSTOJDBC \
--templateProperty project.id=<gcp-project-id> \
--templateProperty gcs.jdbc.input.format=<avro | parquet | orc> \
--templateProperty gcs.jdbc.input.location=<gcs path> \
--templateProperty gcs.jdbc.output.driver=<jdbc driver required in single quotes> \
--templateProperty gcs.jdbc.output.url=<jdbc url along with username and password in single quotes> \
--templateProperty gcs.jdbc.output.table=<jdbc connection table id> \
--templateProperty gcs.jdbc.output.saveMode=<Append|Overwrite|ErrorIfExists|Ignore>

optional parameters:
--templateProperty gcs.jdbc.spark.partitions=<>
--templateProperty gcs.jdbc.output.batchInsertSize=<>

Specifying spark partitions is recommeneded when we have small number of current partitions. By default it will keep the initial partitions set by spark read() which will depend on the block size and number of source files. CAUTION: If you specify a higher number than the current number of partitions, spark will use repartition() for a complete reshuffle, which would add up extra time to your job run.

For the batchInsertSize, a high number is recommended for faster upload to RDBMS in case of bulk loads. By default it is 1000.


Example execution:-

bin/start.sh \
-- --template GCSTOJDBC \
--templateProperty project.id=my-gcp-project \
--templateProperty gcs.jdbc.input.location=gs://my-gcp-project-bucket/empavro \
--templateProperty gcs.jdbc.input.format=avro \
--templateProperty gcs.jdbc.output.table=avrodemo \
--templateProperty gcs.jdbc.output.saveMode=Overwrite \
--templateProperty gcs.jdbc.output.url='jdbc:mysql://192.168.16.3:3306/test?user=root&password=root' \
--templateProperty gcs.jdbc.output.driver='com.mysql.jdbc.Driver' \
--templateProperty gcs.jdbc.spark.partitions=200 \
--templateProperty gcs.jdbc.output.batchInsertSize=100000 \

```

## 5. GCS to GCS

```

GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
bin/start.sh \
-- --template GCSTOGCS \
--templateProperty project.id=<gcp-project-id> \
--templateProperty gcs.gcs.input.format=<avro | parquet | orc> \
--templateProperty gcs.gcs.input.location=<gcs path> \
--templateProperty gcs.gcs.output.format=<avro | parquet | orc> \
--templateProperty gcs.gcs.output.location=<gcs path> \
--templateProperty gcs.gcs.write.mode=<Append|Overwrite|ErrorIfExists|Ignore>
--templateProperty gcs.gcs.temp.table=<temporary table name for data processing> \
--templateProperty gcs.gcs.temp.query=<sql query to process data from temporary table>

Example execution:-

bin/start.sh \
-- --template GCSTOJDBC \
--templateProperty project.id=my-gcp-project \
--templateProperty gcs.gcs.input.location=gs://my-gcp-project-input-bucket/filename.avro \
--templateProperty gcs.gcs.input.format=avro \
--templateProperty gcs.gcs.output.location=gs://my-gcp-project-output-bucket \
--templateProperty gcs.gcs.output.format=csv \
--templateProperty gcs.jdbc.output.saveMode=Overwrite 
--templateProperty gcs.gcs.temp.table=tempTable \
--templateProperty gcs.gcs.temp.query='select * from global_temp.tempTable where sal>1500'

```