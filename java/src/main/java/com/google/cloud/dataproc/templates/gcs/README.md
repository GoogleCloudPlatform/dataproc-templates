## 1. Cloud Storage To BigQuery

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
--templateProperty gcs.bigquery.output.mode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty gcs.bigquery.temp.bucket.name=<bigquery temp bucket name>
```

There are two optional properties as well with "Cloud Storage to BigQuery" Template. Please find below the details :-

```
--templateProperty gcs.bigquery.temp.table='temporary_view_name'
--templateProperty gcs.bigquery.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations while loading data into BigQuery.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

## 2. Cloud Storage To BigTable

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

## 3. Cloud Storage to Spanner
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


## 4. Cloud Storage to JDBC

```
Please download the JDBC Driver of respective database and copy it to GCS bucket location.

export JARS=<GCS location for JDBC connector jar>

GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
bin/start.sh \
-- --template GCSTOJDBC \
--templateProperty project.id=<gcp-project-id> \
--templateProperty gcs.jdbc.input.format=<avro | csv | parquet | orc> \
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

When the JDBC target is PostgreSQL it is recommended to include the connection parameter reWriteBatchedInserts=true in the JDBC URL to provide a significant performance improvement over the default setting.


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

## 5. Cloud Storage to Cloud Storage

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

## 6. Cloud Storage To Mongo:

Download the following MongoDb connectors and copy it to Cloud Storage bucket location:
* [MongoDb Spark Connector](https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector)
* [MongoDb Java Driver](https://mvnrepository.com/artifact/org.mongodb/mongo-java-driver)

```
export GCP_PROJECT=<project-id>\
export REGION=<region>\
export SUBNET=<subnet>\
export GCS_STAGING_LOCATION=<gcs-staging-location-folder>\
export JARS=<gcs-location-to-mongodb-drivers>\

bin/start.sh \
-- --template GCSTOMONGO \
--templateProperty log.level="ERROR" \
--templateProperty gcs.mongodb.input.format=<csv|avro|parquet|json> \
--templateProperty gcs.mongodb.input.location=<gcs-input-location> \
--templateProperty gcs.mongodb.output.uri=<mongodb-output-uri> \
--templateProperty gcs.mongodb.output.database=<database-name>\
--templateProperty gcs.mongodb.output.collection=<collection-name> \
--templateProperty gcs.mongo.output.mode=<Append|Overwrite|ErrorIfExists|Ignore>

```
Example execution:
```
export GCP_PROJECT=mygcpproject
export REGION=us-west1
export SUBNET=projects/mygcpproject/regions/us-west1/subnetworks/test-subnet1
export GCS_STAGING_LOCATION="gs://dataproctemplatesbucket"
export JARS="gs://dataproctemplatesbucket/mongo_dependencies/mongo-java-driver-3.9.1.jar,gs://dataproctemplatesbucket/mongo_jar/mongo-spark-connector_2.12-2.4.0.jar"

bin/start.sh \
-- --template GCSTOMONGO \
--templateProperty log.level="ERROR" \
--templateProperty gcs.mongodb.input.format=avro \
--templateProperty gcs.mongodb.input.location="gs://dataproctemplatesbucket/empavro" \
--templateProperty gcs.mongodb.output.uri="mongodb://1.2.3.4:27017" \
--templateProperty gcs.mongodb.output.database="demo" \
--templateProperty gcs.mongodb.output.collection="test" \
--templateProperty gcs.mongo.output.mode="overwrite"
```

## 7. Text To BigQuery

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
bin/start.sh \
-- --template TEXTTOBIGQUERY \
--templateProperty project.id=<gcp-project-id> \
--templateProperty text.bigquery.input.location=<gcs path for input file> \
--templateProperty text.bigquery.input.compression=<compression file format like gzip or deflate> \
--templateProperty text.bigquery.input.delimiter=<, for csv> \
--templateProperty text.bigquery.output.dataset=<Big query dataset name> \
--templateProperty text.bigquery.output.table=<Big query table name> \
--templateProperty text.bigquery.output.mode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty text.bigquery.temp.bucket=<bigquery temp bucket name>
```

There are two optional properties as well with "Text to BigQuery" Template. Please find below the details :-

```
--templateProperty text.bigquery.temp.table='temporary_view_name'
--templateProperty text.bigquery.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations while loading data into BigQuery.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

