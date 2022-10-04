## Executing Spanner to GCS template

General Execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<region>
export SUBNET=<subnet>
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
bin/start.sh \
-- --template SPANNERTOGCS \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty spanner.gcs.input.spanner.id=<spanner-id> \
--templateProperty spanner.gcs.input.database.id=<database-id> \
--templateProperty spanner.gcs.input.table.id=<table-id> \
--templateProperty spanner.gcs.output.gcs.path=<gcs-path> \
--templateProperty spanner.gcs.output.gcs.saveMode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty spanner.gcs.output.gcs.format=<avro|csv|parquet|json|orc> \
--templateProperty spanner.gcs.input.sql.partitionColumn=<optional-sql-partition-column> \
--templateProperty spanner.gcs.input.sql.lowerBound=<optional-partition-lower-bound-value> \
--templateProperty spanner.gcs.input.sql.upperBound=<optional-partition-lower-bound-value> \
--templateProperty spanner.gcs.input.sql.numPartitions=<optional-partition-partition-number>
```

**Note**: partitionColumn, lowerBound, upperBound and numPartitions must be used together. 
If one is specified then all needs to be specified.

### Export query results as avro
Update`spanner.gcs.input.table.id` property as follows:
```
"spanner.gcs.input.table.id=(select name, age, phone from employee where designation = 'engineer')"
```
There are two optional properties as well with "Spanner to GCS" Template. Please find below the details :-

```
--templateProperty spanner.gcs.temp.table='temporary_view_name' 
--templateProperty spanner.gcs.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into GCS.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

**NOTE** It is required to surround your custom query with parenthesis and parameter name with double quotes.

## Executing Cassandra to GCS Template
### General Execution

```
export REGION=<gcp-region>
export GCP_PROJECT=<gcp-project-name>
export GCS_STAGING_LOCATION=<gcs-staging-location>
export JOB_TYPE=SERVERLESS 
export SUBNET=<dataproc-serverless-subnet>

bin/start.sh \
-- --template CASSANDRATOGCS \
--templateProperty cassandratogcs.input.keyspace=<keyspace-name> \
--templateProperty cassandratogcs.input.table=<input-table-name> \
--templateProperty cassandratogcs.input.host=<cassandra-host-ip> \
--templateProperty cassandratogcs.output.format=<avro|csv|parquet|json|orc> \
--templateProperty cassandratogcs.output.savemode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty cassandratogcs.output.path=<gcs-output-path> 
```
### Example Submission:
```
export REGION=us-central1
export GCP_PROJECT=myproject
export GCS_STAGING_LOCATION=gs://staging
export JOB_TYPE=SERVERLESS 
export SUBNET=projects/myproject/regions/us-central1/subnetworks/default

bin/start.sh \
-- --template CASSANDRATOGCS \
--templateProperty cassandratogcs.input.keyspace=testkeyspace \
--templateProperty cassandratogcs.input.table=testtable \
--templateProperty cassandratogcs.input.host=10.128.2.62 \
--templateProperty cassandratogcs.output.format=csv \
--templateProperty cassandratogcs.output.savemode=append \
--templateProperty cassandratogcs.output.path=gs://myproject/cassandraOutput 
```
One can add additional property to submit query. Please see format below-:
```
--templateProperty cassandratogcs.input.catalog.name=<catalog-name>
--templateProperty cassandratogcs.input.query="select * from <catalog-name>.<keyspace-name>.<table-name>"
```
Note-: ```cassandratogcs.input.catalog.name=<catalog-name>``` is used to provide a name to the connection with Cassandra. This name is used for querying purpose and has a default value of ```casscon``` if nothing is passed. 
To query using default catalog name -:
```
--templateProperty cassandratogcs.input.query="select * from casscon.<keyspace-name>.<table-name>"
```
You can replace the ```casscon``` with your catalog name if it is passed. This is an important step to query the data from Cassandra. Additional details on usage of query can be found in this [link](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md).

## Executing Cassandra to Bigquery Template
### General Execution

```
export REGION=<gcp-region>
export GCP_PROJECT=<gcp-project-name>
export GCS_STAGING_LOCATION=<gcs-staging-location>
export JOB_TYPE=SERVERLESS 
export SUBNET=<dataproc-serverless-subnet>

bin/start.sh \
-- --template CASSANDRATOBQ \
--templateProperty cassandratobq.input.keyspace=<keyspace-name> \
--templateProperty cassandratobq.input.table=<input-table-name> \
--templateProperty cassandratobq.input.host=<cassandra-host-ip> \
--templateProperty cassandratobq.bigquery.location=<dataset>.<table-name> \
--templateProperty cassandratobq.output.mode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty cassandratobq.temp.gcs.location=<gcs-bucket-name> 
```
### Example Submission:
```
export REGION=us-central1
export GCP_PROJECT=myproject
export GCS_STAGING_LOCATION=gs://staging
export JOB_TYPE=SERVERLESS 
export SUBNET=projects/myproject/regions/us-central1/subnetworks/default

bin/start.sh \
-- --template CASSANDRATOBQ \
--templateProperty cassandratobq.input.keyspace=keyspace \
--templateProperty cassandratobq.input.table=table \
--templateProperty cassandratobq.input.host=10.128.0.62 \
--templateProperty cassandratobq.bigquery.location=demodataset.demotable \
--templateProperty cassandratobq.output.mode=Append \
--templateProperty cassandratobq.temp.gcs.location=tempbucket
```
One can add additional property to submit query. Please see format below-:
```
--templateProperty cassandratobq.input.catalog.name=<catalog-name>
--templateProperty cassandratobq.input.query="select * from <catalog-name>.<keyspace-name>.<table-name>"
```
Note-: ```cassandratobq.input.catalog.name=<catalog-name>``` is used to provide a name to the connection with Cassandra. This name is used for querying purpose and has a default value of ```casscon``` if nothing is passed.
To query using default catalog name -:
```
--templateProperty cassandratobq.input.query="select * from casscon.employee.salaries"
```
You can replace the ```casscon``` with your catalog name if it is passed. This is an important step to query the data from Cassandra. Additional details on usage of query can be found in this [link](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md).


## Executing Redshift to GCS template

General Execution:

```
export GCP_PROJECT=<gcp-project-id> 
export REGION=<region>  
export SUBNET=<subnet>   
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS=gs://<cloud-storage-bucket-name>/spark-redshift_<version>.jar,gs://<cloud-storage-bucket-name>/redshift_jdbc_<version>.jar,gs://<cloud-storage-bucket-name>/minimal_json<version>.jar

bin/start.sh \
-- --template REDSHIFTTOGCS \
--templateProperty project.id=<gcp-project-id> \
--templateProperty redshift.aws.input.url=<jdbc:redshift://host-name:port-number/> \
--templateProperty redshift.aws.input.table=<Redshift-table-name> \
--templateProperty redshift.aws.input.temp.dir=<AWS-temp-directory> \
--templateProperty redshift.aws.input.iam.role=<Redshift-S3-IAM-role> \
--templateProperty redshift.aws.input.access.key=<Access-key> \
--templateProperty redshift.aws.input.secret.key=<Secret-key> \
--templateProperty redshift.gcs.output.file.format=<Output-File-Format> \
--templateProperty redshift.gcs.output.file.location=<Output-GCS-location> \
--templateProperty redshift.gcs.output.mode=<Output-GCS-Save-mode>
```

There are two optional properties as well with "Redshift to GCS" Template. Please find below the details :-

```
--templateProperty redshift.gcs.temp.table='temporary_view_name' 
--templateProperty redshift.gcs.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations while loading data into GCS.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

