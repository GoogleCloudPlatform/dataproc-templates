**Pre-requisites:**

* Source Database must be accessible from the Subnet configured.
  * Documentation reference for Network configuration - https://cloud.google.com/dataproc-serverless/docs/concepts/network
* Customize the subnet using following command

  ```
   export SUBNET=projects/<gcp-project>/regions/<region>/subnetworks/<subnet-name>
  ```

## Supported Databases for JDBC
Following databases are supported via Spark JDBC by default:
- DB2
- MySQL / MariaDB
- MS Sql
- Oracle
- PostgreSQL

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
--templateProperty project.id=<gcp-project-id> \
--templateProperty jdbctobq.bigquery.location=<bigquery destination> \
--templateProperty jdbctobq.jdbc.url=<jdbc url> \
--templateProperty jdbctobq.jdbc.driver.class.name=<jdbc driver class name> \
--templateProperty jdbctobq.jdbc.fetchsize=<optional-fetch-size> \
--templateProperty jdbctobq.sql=<input-sql> \
--templateProperty jdbctobq.sql.partitionColumn=<optional-partition-column-name> \
--templateProperty jdbctobq.sql.lowerBound=<optional-partition-start-value> \
--templateProperty jdbctobq.sql.upperBound=<optional-partition-end-value> \
--templateProperty jdbctobq.sql.numPartitions=<optional-partition--number> \
--templateProperty jdbctobq.write.mode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty jdbctobq.temp.gcs.bucket=<temp gcs bucket name>
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

There are two optional properties as well with "JDBC to BigQuery" Template. Please find below the details :-

```
--templateProperty jdbctobq.temp.table='temporary_view_name'
--templateProperty jdbctobq.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into BigQuery.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

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
--templateProperty project.id=<gcp-project-id> \
--templateProperty jdbctogcs.jdbc.url=<jdbc url> \
--templateProperty jdbctogcs.jdbc.driver.class.name=<jdbc-driver-class-name> \
--templateProperty jdbctogcs.jdbc.fetchsize=<optional-fetch-size> \
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

Instead of SQL query, cloud storage path to the SQL file can also be provided. Example,

```
--templateProperty   jdbctogcs.sql.file=gs://my_bkt/sql/demo.sql
```
**Note**: Template property sql and sql.file must not be used together. Either one of them must be provided at a time.

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

There are two optional properties as well with "JDBC to GCS" Template. Please find below the details :-

```
--templateProperty jdbctogcs.temp.table='temporary_view_name'
--templateProperty jdbctogcs.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into GCS.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

***

## 3. JDBC To SPANNER

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
-- --template JDBCTOSPANNER \
--templateProperty project.id=<gcp-project-id> \
--templateProperty jdbctospanner.jdbc.url=<jdbc url> \
--templateProperty jdbctospanner.jdbc.driver.class.name=<jdbc-driver-class-name> \
--templateProperty jdbctospanner.jdbc.fetchsize=<optional-fetch-size> \
--templateProperty jdbctospanner.sql=<input-sql> \
--templateProperty jdbctospanner.sql.partitionColumn=<optional-partition-column-name> \
--templateProperty jdbctospanner.sql.lowerBound=<optional-partition-start-value> \
--templateProperty jdbctospanner.sql.upperBound=<optional-partition-end-value> \
--templateProperty jdbctospanner.sql.numPartitions=<optional-partition-number> \
--templateProperty jdbctospanner.output.instance=<spanner instance id> \
--templateProperty jdbctospanner.output.database=<spanner database id> \
--templateProperty jdbctospanner.output.table=<spanner table id> \
--templateProperty jdbctospanner.output.saveMode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty jdbctospanner.output.primaryKey=<column[(,column)*] - primary key columns needed when creating the table> \
--templateProperty jdbctospanner.output.batchInsertSize=<optional integer>
```

Note: Following is example JDBC URL for mysql database

```
--templateProperty  'jdbctospanner.jdbc.url=jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>'
```

Have SQL query within double quotes. Example,

```
--templateProperty  'jdbctospanner.sql=select * from dbname.tablename'
```

Instead of SQL query, cloud storage path to the SQL file can also be provided. Example,

```
--templateProperty   jdbctospanner.sql.file=gs://my_bkt/sql/demo.sql
```
**Note**: Template property sql and sql.file must not be used together. Either one of them must be provided at a time.

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
    -- --template JDBCTOSPANNER \
    --templateProperty project.id=my-gcp-proj \
    --templateProperty 'jdbctospanner.jdbc.url=jdbc:mysql://192.168.16.3:3306/MyCloudSQLDB?user=root&password=root' \
    --templateProperty jdbctospanner.jdbc.driver.class.name=com.mysql.cj.jdbc.Driver \
    --templateProperty 'jdbctospanner.sql=SELECT * FROM MyCloudSQLDB.table1' \
    --templateProperty jdbctospanner.output.instance='spanner-instance-id' \
    --templateProperty jdbctospanner.output.database='spanner-database-name' \
    --templateProperty jdbctospanner.output.table='spanner-table-name' \
    --templateProperty jdbctospanner.output.saveMode=Overwrite \
    --templateProperty jdbctospanner.output.primaryKey='primary-key' \
    --templateProperty jdbctospanner.output.batchInsertSize=200

There are two optional properties as well with "JDBC to SPANNER" Template. Please find below the details :-

```
--templateProperty jdbctospanner.temp.table='temporary_view_name'
--templateProperty jdbctospanner.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into GCS.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"
