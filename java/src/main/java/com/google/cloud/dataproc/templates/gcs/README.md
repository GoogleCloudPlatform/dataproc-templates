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
--templateProperty gcs.bigquery.temp.table='table_name' 
--templateProperty gcs.bigquery.temp.query='select * from global_temp.table_name'
```
These properties are responsible for applying some spark sql transformations while loading data into BigQuery.
The only thing needs to keep in mind is that, the name of dataset and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

## 2. GCS to Spanner
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


## 3. GCS to JDBC

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

Example execution:-

bin/start.sh \
-- --template GCSTOJDBC \
--templateProperty project.id=my-gcp-project \
--templateProperty gcs.jdbc.input.location=gs://my-gcp-project-bucket/empavro \
--templateProperty gcs.jdbc.input.format=avro \
--templateProperty gcs.jdbc.output.table=avrodemo \
--templateProperty gcs.jdbc.output.saveMode=Overwrite \
--templateProperty gcs.jdbc.output.url='jdbc:mysql://192.168.16.3:3306/test?user=root&password=root' \
--templateProperty gcs.jdbc.output.driver='com.mysql.jdbc.Driver'

```