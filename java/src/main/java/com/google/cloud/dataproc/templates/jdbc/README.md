# Prerequisites

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
- Microsoft SQL Server
- Oracle
- PostgreSQL

## Required JAR files

These templates requires the JDBC jar file to be available in the Dataproc cluster.
User has to download the required jar file and host it inside a Cloud Storage Bucket, so that it could be referred during the execution of code.

wget command to download JDBC jar file is as follows :-

* MySQL
```
wget http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.30.tar.gz
```
* PostgreSQL
```
wget https://jdbc.postgresql.org/download/postgresql-42.2.6.jar
```
* Microsoft SQL Server
```
wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/6.4.0.jre8/mssql-jdbc-6.4.0.jre8.jar
```
* Oracle
```
wget https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.7.0.0/ojdbc8-21.7.0.0.jar
```

Once the jar file gets downloaded, please upload the file into a Cloud Storage Bucket and export the below variable

```
export JARS=<gcs-bucket-location-containing-jar-file>
```

## JDBC URL syntax

* MySQL
```
jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>
```
* PostgreSQL
```
jdbc:postgresql://<hostname>:<port>/<dbname>?user=<username>&password=<password>
```
* Microsoft SQL Server
```
jdbc:sqlserver://<hostname>:<port>;databaseName=<dbname>;user=<username>;password=<password>
```
* Oracle
```
jdbc:oracle:thin:@//<hostname>:<port>/<dbservice>?user=<username>&password=<password>
```

## Other important properties

* Driver Class

  * MySQL
    ```
    jdbctojdbc.input.driver="com.mysql.cj.jdbc.Driver"
    ```
  * PostgreSQL
    ```
    jdbctojdbc.input.driver="org.postgresql.Driver"
    ```
  * Microsoft SQL Server
    ```
    jdbctojdbc.input.driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ```
  * Oracle
    ```
    jdbctojdbc.input.driver="oracle.jdbc.driver.OracleDriver"

* Additional execution details [refer spark jdbc doc](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

## 1. JDBC To BigQuery

Note - Add dependency jars specific to database in JARS variable.

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
--templateProperty jdbctobq.jdbc.sessioninitstatement=<optional-session-init-statement> \
--templateProperty jdbctobq.sql=<input-sql> \
--templateProperty jdbctobq.sql.partitionColumn=<optional-partition-column-name> \
--templateProperty jdbctobq.sql.lowerBound=<optional-partition-start-value> \
--templateProperty jdbctobq.sql.upperBound=<optional-partition-end-value> \
--templateProperty jdbctobq.sql.numPartitions=<optional-partition--number> \
--templateProperty jdbctobq.write.mode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty jdbctobq.temp.gcs.bucket=<temp cloud storage bucket name>
```

**Note**: Following is example JDBC URL for MySQL database:

```
--templateProperty  jdbctobq.jdbc.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>"
```

**Note**: Have SQL query within double quotes, for example:

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

**Note**: sessioninitstatement is a custom SQL statement to execute in each reader database session, for example:
```
jdbctobq.jdbc.sessioninitstatement="BEGIN DBMS_APPLICATION_INFO.SET_MODULE('Dataproc Templates','JDBCTOBQ'); END;"
```

***

## 2. JDBC To Cloud Storage

Note - Add dependency jars specific to database in JARS variable.



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
--templateProperty jdbctogcs.jdbc.sessioninitstatement=<optional-session-init-statement> \
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

**Note**: Following is example JDBC URL for MySQL database:

```
--templateProperty  'jdbctogcs.jdbc.url=jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>'
```

**Note**: Have SQL query within double quotes, for example:

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

**Note**: sessioninitstatement is a custom SQL statement to execute in each reader database session, for example:
```
jdbctogcs.jdbc.sessioninitstatement="BEGIN DBMS_APPLICATION_INFO.SET_MODULE('Dataproc Templates','JDBCTOBQ'); END;"
```

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

There are two optional properties as well with "JDBC to Cloud Storage" Template. Please find below the details :-

```
--templateProperty jdbctogcs.temp.table='temporary_view_name'
--templateProperty jdbctogcs.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into Cloud Storage.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

***

## 3. JDBC To SPANNER

Note - Add dependency jars specific to database in JARS variable.

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
--templateProperty jdbctospanner.jdbc.sessioninitstatement=<optional-session-init-statement> \
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
--templateProperty jdbctospanner.output.batch.size=<optional integer>
```

**Note**: Following is example JDBC URL for MySQL database:

```
--templateProperty  'jdbctospanner.jdbc.url=jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>'
```

**Note**: Have SQL query within double quotes, for example:

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

**Note**: sessioninitstatement is a custom SQL statement to execute in each reader database session, for example:
```
jdbctospanner.jdbc.sessioninitstatement="BEGIN DBMS_APPLICATION_INFO.SET_MODULE('Dataproc Templates','JDBCTOBQ'); END;"
```

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
    --templateProperty jdbctospanner.output.batch.size=200

There are two optional properties as well with "JDBC to SPANNER" Template. Please find below the details :-

```
--templateProperty jdbctospanner.temp.table='temporary_view_name'
--templateProperty jdbctospanner.temp.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into Cloud Storage.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

# 4. JDBC To JDBC

Template for reading data from JDBC table and writing them to a JDBC table. It supports reading partition tables and write into partitioned or non-partitioned tables.

## Mandatory Arguments

* `jdbctojdbc.input.url`: JDBC input URL
* `jdbctojdbc.input.driver`: JDBC input driver name
* `jdbctojdbc.input.table`: JDBC input table name
* `jdbctojdbc.output.url`: JDBC output url. When the JDBC target is PostgreSQL it is recommended to include the connection parameter reWriteBatchedInserts=true in the URL to provide a significant performance improvement over the default setting.
* `jdbctojdbc.output.driver`: JDBC output driver name
* `jdbctojdbc.output.table`: JDBC output table name

## Optional Arguments

* `jdbctojdbc.input.partitioncolumn`: JDBC input table partition column name
* `jdbctojdbc.input.lowerbound`: JDBC input table partition column lower bound which is used to decide the partition stride
* `jdbctojdbc.input.upperbound`: JDBC input table partition column upper bound which is used to decide the partition stride
* `jdbctojdbc.numpartitions`: The maximum number of partitions that can be used for parallelism in table reading and writing. Same value will be used for both input and output jdbc connection. Default set to 10
* `jdbctojdbc.input.fetchsize`: Determines how many rows to fetch per round trip
* `jdbctojdbc.output.create_table.option`: This option allows setting of database-specific table and partition options when creating a output table
* `jdbctojdbc.output.mode`: Output write mode. One of: append, overwrite, ignore, errorifexists. Defaults to append
* `jdbctojdbc.sessioninitstatement`: After each database session is opened to the remote DB and before starting to read data, this option executes a custom SQL statement (or a PL/SQL block). Use this to implement session initialization code
* `jdbctojdbc.output.primary.key`: Specify primary key column for output table. Column mentioned should **not** contain duplicate values, otherwise an error will be thrown
* `jdbctojdbc.output.batch.size`: JDBC output batch size. Default set to 1000

## General Execution

```
export GCP_PROJECT=<gcp-project-id> \
export REGION=<region>  \
export SUBNET=<subnet>   \
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
export JARS=<gcs_path_to_jdbc_jar_files>

bin/start.sh \
-- --template JDBCTOJDBC \
--templateProperty jdbctojdbc.input.url=<jdbc-input-url> \
--templateProperty jdbctojdbc.input.driver=<jdbc-input-driver-class-name> \
--templateProperty jdbctojdbc.input.table=<jdbc-input-table-or-query> \
--templateProperty jdbctojdbc.output.url=<jdbc-output-url> \
--templateProperty jdbctojdbc.output.driver=<jdbc-output-driver-class-name> \
--templateProperty jdbctojdbc.output.table=<jdbc-output-table> \
--templateProperty jdbctojdbc.input.fetchsize=<optional-fetch-size> \
--templateProperty jdbctojdbc.output.mode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty jdbctojdbc.output.batch.size=<optional-batchsize-integer> \
--templateProperty jdbctojdbc.input.partitioncolumn=<optional-partition-column-name> \
--templateProperty jdbctojdbc.input.lowerbound=<optional-partition-start-value> \
--templateProperty jdbctojdbc.input.upperbound=<optional-partition-end-value> \
--templateProperty jdbctojdbc.numpartitions=<optional-partition-number> \
--templateProperty jdbctojdbc.output.create.table.option=<optional-output-table-properties> \
--templateProperty jdbctojdbc.output.primary.key=<optional-output-table-primary-key-column-name> \
--templateProperty jdbctojdbc.sessioninitstatement=<optional-session-init-statement> \

```

## Note:
* You can specify the target table properties such as partition column using below property. This is useful when target table is not present or when write mode=overwrite, and you need the target table to be created as partitioned table.
  * MySQL
    ```
    jdbctojdbc.output.create_table.option="PARTITION BY RANGE(id)  (PARTITION p0 VALUES LESS THAN (5),PARTITION p1 VALUES LESS THAN (10),PARTITION p2 VALUES LESS THAN (15),PARTITION p3 VALUES LESS THAN MAXVALUE)"
    ```
  * PostgreSQL
    ```
    jdbctojdbc.output.create_table.option="PARTITION BY RANGE(id);CREATE TABLE po0 PARTITION OF <table_name> FOR VALUES FROM (MINVALUE) TO (5);CREATE TABLE po1 PARTITION OF <table_name> FOR VALUES FROM (5) TO (10);CREATE TABLE po2 PARTITION OF <table_name> FOR VALUES FROM (10) TO (15);CREATE TABLE po3 PARTITION OF <table_name> FOR VALUES FROM (15) TO (MAXVALUE);"
    ```

* You can either specify the source table name or have SQL query within double quotes. Example,

  ```
  jdbctojdbc.input.table="employees"
  jdbctojdbc.input.table="(select * from employees where dept_id>10) as employees"
  ```

* partitionColumn, lowerBound, upperBound and numPartitions must be used together.
  If one is specified then all needs to be specified.


* The column name specified for `jdbctojdbc.output.primary.key` for Microsoft SQL must not be nullable. If nullable below error will be thrown on running the template - 
  ```
  com.microsoft.sqlserver.jdbc.SQLServerException: Cannot define PRIMARY KEY constraint on nullable column in table
  ```

* `jdbctojdbc.sessioninitstatement` is a custom SQL statement to execute in each reader database session, for example:
  ```
  jdbctojdbc.sessioninitstatement="BEGIN DBMS_APPLICATION_INFO.SET_MODULE('Dataproc Templates','JDBCTOJDBC'); END;"
  ```

* There are two optional properties as well with "JDBC to JDBC" Template. Please find below the details :-

  ```
  --templateProperty jdbctojdbc.temp.view.name='temporary_view_name'
  --templateProperty jdbctojdbc.sql.query='select * from global_temp.temporary_view_name'
  ```
  These properties are responsible for applying some spark sql transformations before loading data into JDBC.
  The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"


## Example execution:

```
export GCP_PROJECT=my-gcp-proj
export REGION=us-central1
export GCS_STAGING_LOCATION=gs://my-gcp-proj/staging
export SUBNET=projects/my-gcp-proj/regions/us-central1/subnetworks/default
export JARS="gs://my-gcp-proj/jars/mysql-connector-java-8.0.29.jar,gs://my-gcp-proj/jars/postgresql-42.2.6.jar,gs://my-gcp-proj/jars/mssql-jdbc-6.4.0.jre8.jar,gs://my-gcp-proj/jars/ojdbc8-21.7.0.0.jar"
```
* MySQL to MySQL
```
bin/start.sh \
-- --template JDBCTOJDBC \
--templateProperty jdbctojdbc.input.url="jdbc:mysql://1.1.1.1:3306/db-test?user=test&password=password" \
--templateProperty jdbctojdbc.input.driver="com.mysql.cj.jdbc.Driver" \
--templateProperty jdbctojdbc.input.table="employees" \
--templateProperty jdbctojdbc.output.url="jdbc:mysql://1.1.1.1:3306/db-test?user=test&password=password" \
--templateProperty jdbctojdbc.output.driver="com.mysql.cj.jdbc.Driver" \
--templateProperty jdbctojdbc.output.table="employees_output" \
--templateProperty jdbctojdbc.input.partitioncolumn="id" \
--templateProperty jdbctojdbc.input.lowerbound="1" \
--templateProperty jdbctojdbc.input.upperbound="10" \
--templateProperty jdbctojdbc.numpartitions="4" \
--templateProperty jdbctojdbc.output.mode="Overwrite" \
--templateProperty jdbctojdbc.output.batch.size="100" \
--templateProperty jdbctojdbc.output.create.table.option="PARTITION BY RANGE(id)  (PARTITION p0 VALUES LESS THAN (5),PARTITION p1 VALUES LESS THAN (10),PARTITION p2 VALUES LESS THAN (15),PARTITION p3 VALUES LESS THAN MAXVALUE)" \
--templateProperty jdbctojdbc.temp.view.name="employees_view" \
--templateProperty jdbctojdbc.sql.query="select * from global_temp.employees_view where id <= 200"
```
