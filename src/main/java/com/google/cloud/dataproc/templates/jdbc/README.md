**Pre-requisites:**

* Source Database must be accessible from the Subnet configured. 
  * Documentation reference for Network configuration - https://cloud.google.com/dataproc-serverless/docs/concepts/network 
* Customize the subnet using following command

  ```
   export SUBNET=projects/<gcp-project>/regions/<region>/subnetworks/test-subnet1
  ```


## 1. JDBC To BigQuery

Note - Add dependency jar's specific to database in jars variable. 

Example: export JARS=gs://<bucket_name>/mysql-connector-java.jar

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
export JARS=<gcs_path_to_jar_files> \

bin/start.sh \
-- --template JDBCTOBIGQUERY \
--templateProperty jdbctobq.bigquery.location=<bigquery destination> \
--templateProperty jdbctobq.jdbc.url=<jdbc url> \
--templateProperty jdbctobq.jdbc.driver.class.name=<jdbc driver class name> \
--templateProperty jdbctobq.jdbc.properties=<jdbc properties in json format> \
--templateProperty jdbctobq.input.table=<source table> \
--templateProperty jdbctobq.input.db=<source database> \
--templateProperty jdbctobq.write.mode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty jdbctobq.spark.sql.warehouse.dir=<gcs path> \
```

***

## 2. JDBC To GCS

Note - Add dependency jar's specific to database in jars variable.

Example: export JARS=gs://<bucket_name>/mysql-connector-java.jar

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
export JARS=<gcs_path_to_jar_files> \

bin/start.sh \
-- --template JDBCTOGCS \
--templateProperty jdbctobq.jdbc.url=<jdbc url> \
--templateProperty  jdbctogcs.jdbc.driver.class.name=<jdbc-driver-class-name> \
--templateProperty  jdbctogcs.output.location=<gcs-ouput-location> \
--templateProperty  jdbctogcs.output.format=<optional_output-format> \
--templateProperty  jdbctogcs.write.mode=<optional_write-mode> \
--templateProperty  jdbctogcs.sql=<input-sql> \
--templateProperty  jdbctogcs.partition.col=<optional_partition-col> \
```

Note: Following is example JDBC URL for mysql database

```
--templateProperty  jdbctogcs.jdbc.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>"
```

Have SQL query within double quotes. Example,

```
--templateProperty  jdbctogcs.sql="select * from dbname.tablename"
```