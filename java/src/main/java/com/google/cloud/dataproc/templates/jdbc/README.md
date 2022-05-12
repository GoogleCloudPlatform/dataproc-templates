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
export GCP_PROJECT=<gcp-project-id> \
export REGION=<region>  \
export SUBNET=<subnet>   \
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
export JARS=<gcs_path_to_jar_files>

bin/start.sh \
-- --template JDBCTOBIGQUERY \
--templateProperty jdbctobq.bigquery.location=<bigquery destination> \
--templateProperty jdbctobq.jdbc.url=<jdbc url> \
--templateProperty jdbctobq.jdbc.driver.class.name=<jdbc driver class name> \
--templateProperty jdbctobq.sql=<input-sql> \
--templateProperty jdbctobq.sql.partitionColumn=<optional-partition-column-name> \
--templateProperty jdbctobq.sql.lowerBound=<optional-partition-start-value> \
--templateProperty jdbctobq.sql.upperBound=<optional-partition-end-value> \
--templateProperty jdbctobq.sql.numPartitions=<optional-partition--number> \
--templateProperty jdbctobq.write.mode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty jdbctobq.temp.gcs.bucket=<gcs path>
```

Note: Following is example JDBC URL for mysql database

```
--templateProperty  jdbctobq.jdbc.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>"
```

Have SQL query within double quotes. Example,

```
--templateProperty  jdbctobq.sql="select * from dbname.tablename"
```

**Note**: partitionColumn, lowerBound, upperBound and numPartitions must be used together.
If one is specified then all needs to be specified.

Additional execution details [refer spark jdbc doc](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

***

## 2. JDBC To GCS

Note - Add dependency jar's specific to database in jars variable.

Example: export JARS=gs://<bucket_name>/mysql-connector-java.jar

General Execution  

```
export GCP_PROJECT=<gcp-project-id> \
export REGION=<region>  \
export SUBNET=<subnet>   \
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
export JARS=<gcs_path_to_jdbc_jar_files>

bin/start.sh \
-- --template JDBCTOGCS \
--templateProperty jdbctogcs.jdbc.url=<jdbc url> \
--templateProperty jdbctogcs.jdbc.driver.class.name=<jdbc-driver-class-name> \
--templateProperty jdbctogcs.output.location=<gcs-ouput-location> \
--templateProperty jdbctogcs.output.format=<csv|avro|orc|json|parquet> \
--templateProperty jdbctogcs.write.mode=<optional_write-mode> \
--templateProperty jdbctogcs.sql=<input-sql> \
--templateProperty jdbctogcs.sql.partitionColumn=<optional-partition-column-name> \
--templateProperty jdbctogcs.sql.lowerBound=<optional-partition-start-value> \
--templateProperty jdbctogcs.sql.upperBound=<optional-partition-end-value> \
--templateProperty jdbctogcs.sql.numPartitions=<optional-partition-number> \
--templateProperty jdbctogcs.output.partition.col=<optional_partition-col>
```

Note: Following is example JDBC URL for mysql database

```
--templateProperty  'jdbctogcs.jdbc.url=jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>'
```

Have SQL query within double quotes. Example,

```
--templateProperty  'jdbctogcs.sql=select * from dbname.tablename'
```

**Note**: partitionColumn, lowerBound, upperBound and numPartitions must be used together. 
If one is specified then all needs to be specified.

Additional execution details [refer spark jdbc doc](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

Example execution:


    export GCP_PROJECT=my-gcp-proj \
    export REGION=us-central1  \
    export SUBNET=projects/my-gcp-proj/regions/us-central1/subnetworks/default   \
    export GCS_STAGING_LOCATION=gs://my-gcp-proj/mysql-export/staging \
    export JARS=gs://my-gcp-proj/mysql-export/mysql-connector-java-8.0.17.jar

    bin/start.sh \
    -- --template JDBCTOGCS \
    --templateProperty 'jdbctogcs.jdbc.url=jdbc:mysql://192.168.16.3:3306/MyCloudSQLDB?user=root&password=root' \
    --templateProperty jdbctogcs.jdbc.driver.class.name=com.mysql.cj.jdbc.Driver \
    --templateProperty jdbctogcs.output.location=gs://my-gcp-proj/mysql-export/export/table1_export \
    --templateProperty jdbctogcs.output.format=avro \
    --templateProperty jdbctogcs.write.mode=OVERWRITE \
    --templateProperty 'jdbctogcs.sql=SELECT * FROM MyCloudSQLDB.table1'
