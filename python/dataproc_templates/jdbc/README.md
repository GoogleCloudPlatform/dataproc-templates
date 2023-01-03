# Supported Databases for JDBC
Following databases are supported via Spark JDBC by default:
- DB2
- MySQL / MariaDB
- MS Sql
- Oracle
- PostgreSQL

# Prerequisites

## Required JAR files

These templates requires the JDBC jar file to be available in the Dataproc cluster.
User has to download the required jar file and host it inside a GCS Bucket, so that it could be referred during the execution of code.

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

Once the jar file gets downloaded, please upload the file into a GCS Bucket and export the below variable

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
    ```

* You can either specify the source table name or have SQL query within double quotes. Example,

```
jdbctojdbc.input.table="employees"
jdbctojdbc.input.table="(select * from employees where dept_id>10) as employees"
```

* partitionColumn, lowerBound, upperBound and numPartitions must be used together. If one is specified then all needs to be specified.

* Additional execution details [refer spark jdbc doc](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

# 1. JDBC To JDBC

Template for reading data from JDBC table and writing them to a JDBC table. It supports reading partition tabels and write into partitioned or non-partitioned tables.

## Arguments

* `jdbctojdbc.input.url`: JDBC input URL
* `jdbctojdbc.input.driver`: JDBC input driver name
* `jdbctojdbc.input.table`: JDBC input table name
* `jdbctojdbc.output.url`: JDBC output url
* `jdbctojdbc.output.driver`: JDBC output driver name
* `jdbctojdbc.output.table`: JDBC output table name
* `jdbctojdbc.input.partitioncolumn` (Optional): JDBC input table partition column name
* `jdbctojdbc.input.lowerbound` (Optional): JDBC input table partition column lower bound which is used to decide the partition stride
* `jdbctojdbc.input.upperbound` (Optional): JDBC input table partition column upper bound which is used to decide the partition stride
* `jdbctojdbc.numpartitions` (Optional): The maximum number of partitions that can be used for parallelism in table reading and writing. Same value will be used for both input and output jdbc connection. Default set to 10
* `jdbctojdbc.input.fetchsize` (Optional): Determines how many rows to fetch per round trip
* `jdbctojdbc.output.create_table.option` (Optional): This option allows setting of database-specific table and partition options when creating a output table
* `jdbctojdbc.output.mode` (Optional): Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `jdbctojdbc.output.batch.size` (Optional): JDBC output batch size. Default set to 1000

## Usage

```
$ python main.py --template JDBCTOJDBC --help

usage: main.py --template JDBCTOJDBC \
    --jdbctojdbc.input.url JDBCTOJDBC.INPUT.URL \
    --jdbctojdbc.input.driver JDBCTOJDBC.INPUT.DRIVER \
    --jdbctojdbc.input.table JDBCTOJDBC.INPUT.TABLE \
    --jdbctojdbc.output.url JDBCTOJDBC.OUTPUT.URL \
    --jdbctojdbc.output.driver JDBCTOJDBC.OUTPUT.DRIVER \
    --jdbctojdbc.output.table JDBCTOJDBC.OUTPUT.TABLE \

optional arguments:
    -h, --help            show this help message and exit
    --jdbctojdbc.input.partitioncolumn JDBCTOJDBC.INPUT.PARTITIONCOLUMN \
    --jdbctojdbc.input.lowerbound JDBCTOJDBC.INPUT.LOWERBOUND \
    --jdbctojdbc.input.upperbound JDBCTOJDBC.INPUT.UPPERBOUND \
    --jdbctojdbc.numpartitions JDBCTOJDBC.NUMPARTITIONS \
    --jdbctojdbc.input.fetchsize JDBCTOJDBC.INPUT.FETCHSIZE \
    --jdbctojdbc.output.create_table.option JDBCTOJDBC.OUTPUT.CREATE_TABLE.OPTION \
    --jdbctojdbc.output.mode {overwrite,append,ignore,errorifexists} \
    --jdbctojdbc.output.batch.size JDBCTOJDBC.OUTPUT.BATCH.SIZE \
```
## Note:
* You can specify the target table properties such as partition column using below property. This is useful when target table is not present or when write mode=overwrite and you need the target table to be created as partitioned table.

    * MySQL
    ```
    jdbctojdbc.output.create_table.option="PARTITION BY RANGE(id)  (PARTITION p0 VALUES LESS THAN (5),PARTITION p1 VALUES LESS THAN (10),PARTITION p2 VALUES LESS THAN (15),PARTITION p3 VALUES LESS THAN MAXVALUE)"
    ```
    * PostgreSQL
    ```
    jdbctojdbc.output.create_table.option="PARTITION BY RANGE(id);CREATE TABLE po0 PARTITION OF <table_name> FOR VALUES FROM (MINVALUE) TO (5);CREATE TABLE po1 PARTITION OF <table_name> FOR VALUES FROM (5) TO (10);CREATE TABLE po2 PARTITION OF <table_name> FOR VALUES FROM (10) TO (15);CREATE TABLE po3 PARTITION OF <table_name> FOR VALUES FROM (15) TO (MAXVALUE);"
    ```

## General execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs staging location>
export SUBNET=<subnet>
export JARS="<gcs_path_to_jdbc_jar_files>/mysql-connector-java-8.0.29.jar,<gcs_path_to_jdbc_jar_files>/postgresql-42.2.6.jar,<gcs_path_to_jdbc_jar_files>/mssql-jdbc-6.4.0.jre8.jar"

./bin/start.sh \
-- --template=JDBCTOJDBC \
--jdbctojdbc.input.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>" \
--jdbctojdbc.input.driver=<jdbc-driver-class-name> \
--jdbctojdbc.input.table=<input table name or subquery with where clause filter> \
--jdbctojdbc.input.partitioncolumn=<optional-partition-column-name> \
--jdbctojdbc.input.lowerbound=<optional-partition-start-value>  \
--jdbctojdbc.input.upperbound=<optional-partition-end-value>  \
--jdbctojdbc.numpartitions=<optional-partition-number> \
--jdbctojdbc.input.fetchsize=<optional-fetch-size> \
--jdbctojdbc.output.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>" \
--jdbctojdbc.output.driver=<jdbc-driver-class-name> \
--jdbctojdbc.output.table=<output-table-name> \
--jdbctojdbc.output.create_table.option=<optional-output-table-properties> \
--jdbctojdbc.output.mode=<optional-write-mode> \
--jdbctojdbc.output.batch.size=<optional-batch-size>
```

## Example execution:

```
export GCP_PROJECT=my-gcp-proj
export REGION=us-central1
export GCS_STAGING_LOCATION=gs://my-gcp-proj/staging
export SUBNET=projects/my-gcp-proj/regions/us-central1/subnetworks/default
export JARS="gs://my-gcp-proj/jars/mysql-connector-java-8.0.29.jar,gs://my-gcp-proj/jars/postgresql-42.2.6.jar,gs://my-gcp-proj/jars/mssql-jdbc-6.4.0.jre8.jar"
```
* MySQL to MySQL
```
./bin/start.sh \
-- --template=JDBCTOJDBC \
--jdbctojdbc.input.url="jdbc:mysql://1.1.1.1:3306/mydb?user=root&password=password123" \
--jdbctojdbc.input.driver="com.mysql.cj.jdbc.Driver" \
--jdbctojdbc.input.table="(select * from employees where id <10) as employees" \
--jdbctojdbc.input.partitioncolumn=id \
--jdbctojdbc.input.lowerbound="1" \
--jdbctojdbc.input.upperbound="10" \
--jdbctojdbc.numpartitions="4" \
--jdbctojdbc.output.url="jdbc:mysql://1.1.1.1:3306/mydb?user=root&password=password123" \
--jdbctojdbc.output.driver="com.mysql.cj.jdbc.Driver" \
--jdbctojdbc.output.table="employees_out" \
--jdbctojdbc.output.create_table.option="PARTITION BY RANGE(id)  (PARTITION p0 VALUES LESS THAN (5),PARTITION p1 VALUES LESS THAN (10),PARTITION p2 VALUES LESS THAN (15),PARTITION p3 VALUES LESS THAN MAXVALUE)" \
--jdbctojdbc.output.mode="overwrite" \
--jdbctojdbc.output.batch.size="1000"
```

* PostgreSQL to PostgreSQL
```
./bin/start.sh \
-- --template=JDBCTOJDBC \
--jdbctojdbc.input.url="jdbc:postgresql://1.1.1.1:5432/postgres?user=postgres&password=password123" \
--jdbctojdbc.input.driver="org.postgresql.Driver" \
--jdbctojdbc.input.table="(select * from employees) as employees" \
--jdbctojdbc.input.partitioncolumn=id \
--jdbctojdbc.input.lowerbound="11" \
--jdbctojdbc.input.upperbound="20" \
--jdbctojdbc.numpartitions="4" \
--jdbctojdbc.output.url="jdbc:postgresql://1.1.1.1:5432/postgres?user=postgres&password=password123" \
--jdbctojdbc.output.driver="org.postgresql.Driver" \
--jdbctojdbc.output.table="employees_out" \
--jdbctojdbc.output.create_table.option="PARTITION BY RANGE(id);CREATE TABLE po0 PARTITION OF employees_out FOR VALUES FROM (MINVALUE) TO (5);CREATE TABLE po1 PARTITION OF employees_out FOR VALUES FROM (5) TO (10);CREATE TABLE po2 PARTITION OF employees_out FOR VALUES FROM (10) TO (15);CREATE TABLE po3 PARTITION OF employees_out FOR VALUES FROM (15) TO (MAXVALUE);" \
--jdbctojdbc.output.mode="overwrite" \
--jdbctojdbc.output.batch.size="1000"
```

* Microsoft SQL Server to Microsoft SQL Server
```
./bin/start.sh \
-- --template=JDBCTOJDBC \
--jdbctojdbc.input.url="jdbc:sqlserver://1.1.1.1:1433;databaseName=mydb;user=sqlserver;password=password123" \
--jdbctojdbc.input.driver="com.microsoft.sqlserver.jdbc.SQLServerDriver" \
--jdbctojdbc.input.table="employees" \
--jdbctojdbc.input.partitioncolumn=id \
--jdbctojdbc.input.lowerbound="11" \
--jdbctojdbc.input.upperbound="20" \
--jdbctojdbc.numpartitions="4" \
--jdbctojdbc.output.url="jdbc:sqlserver://1.1.1.1:1433;databaseName=mydb;user=sqlserver;password=password123" \
--jdbctojdbc.output.driver="com.microsoft.sqlserver.jdbc.SQLServerDriver" \
--jdbctojdbc.output.table="employees_out" \
--jdbctojdbc.output.mode="overwrite" \
--jdbctojdbc.output.batch.size="1000"
```

* MySQL to PostgreSQL

```
./bin/start.sh \
-- --template=JDBCTOJDBC \
--jdbctojdbc.input.url="jdbc:mysql://1.1.1.1:3306/mydb?user=root&password=password123" \
--jdbctojdbc.input.driver="com.mysql.cj.jdbc.Driver" \
--jdbctojdbc.input.table="employees" \
--jdbctojdbc.input.partitioncolumn=id \
--jdbctojdbc.input.lowerbound="11" \
--jdbctojdbc.input.upperbound="20" \
--jdbctojdbc.numpartitions="4" \
--jdbctojdbc.output.url="jdbc:postgresql://1.1.1.1:5432/postgres?user=postgres&password=password123" \
--jdbctojdbc.output.driver="org.postgresql.Driver" \
--jdbctojdbc.output.table="employees_out" \
--jdbctojdbc.output.mode="overwrite" \
--jdbctojdbc.output.batch.size="1000"
```

* MySQL to Microsoft SQL Server

```
./bin/start.sh \
-- --template=JDBCTOJDBC \
--jdbctojdbc.input.url="jdbc:mysql://1.1.1.1:3306/mydb?user=root&password=password123" \
--jdbctojdbc.input.driver="com.mysql.cj.jdbc.Driver" \
--jdbctojdbc.input.table="employees" \
--jdbctojdbc.input.partitioncolumn=id \
--jdbctojdbc.input.lowerbound="11" \
--jdbctojdbc.input.upperbound="20" \
--jdbctojdbc.numpartitions="4" \
--jdbctojdbc.output.url="jdbc:sqlserver://1.1.1.1:1433;databaseName=mydb;user=sqlserver;password=password123" \
--jdbctojdbc.output.driver="com.microsoft.sqlserver.jdbc.SQLServerDriver" \
--jdbctojdbc.output.table="employees_out" \
--jdbctojdbc.output.mode="overwrite" \
--jdbctojdbc.output.batch.size="1000"
```

* Oracle to PostgreSQL

```
./bin/start.sh \
-- --template=JDBCTOJDBC \
--jdbctojdbc.input.url="jdbc:oracle:thin:@//1.1.1.1:1521/mydb?user=hr&password=password123" \
--jdbctojdbc.input.driver="oracle.jdbc.driver.OracleDriver" \
--jdbctojdbc.input.table="employees" \
--jdbctojdbc.input.partitioncolumn=id \
--jdbctojdbc.input.lowerbound="11" \
--jdbctojdbc.input.upperbound="20" \
--jdbctojdbc.numpartitions="4" \
--jdbctojdbc.input.fetchsize="200" \
--jdbctojdbc.output.url="jdbc:postgresql://1.1.1.1:5432/postgres?user=postgres&password=password123" \
--jdbctojdbc.output.driver="org.postgresql.Driver" \
--jdbctojdbc.output.table="employees_out" \
--jdbctojdbc.output.mode="overwrite" \
--jdbctojdbc.output.batch.size="1000"
```

There are two optional properties as well with "Hive to JDBC" Template. Please find below the details :-

```
--templateProperty jdbctojdbc.temp.view.name='temporary_view_name'
--templateProperty jdbctojdbc.sql.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into JDBC.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"


# 2. JDBC To GCS

Template for reading data from JDBC table and writing into files in Google Cloud Storage. It supports reading partition tabels and supports writing in JSON, CSV, Parquet and Avro formats.

## Arguments

* `jdbctogcs.input.url`: JDBC input URL
* `jdbctogcs.input.driver`: JDBC input driver name
* `jdbctogcs.input.table`: JDBC input table name
* `jdbctogcs.output.location`: GCS location for output files (format: `gs://BUCKET/...`)
* `jdbctogcs.output.format`: Output file format (one of: avro,parquet,csv,json)
* `jdbctogcs.input.partitioncolumn` (Optional): JDBC input table partition column name
* `jdbctogcs.input.lowerbound` (Optional): JDBC input table partition column lower bound which is used to decide the partition stride
* `jdbctogcs.input.upperbound` (Optional): JDBC input table partition column upper bound which is used to decide the partition stride
* `jdbctogcs.numpartitions` (Optional): The maximum number of partitions that can be used for parallelism in table reading and writing. Same value will be used for both input and output jdbc connection. Default set to 10
* `jdbctogcs.input.fetchsize` (Optional): Determines how many rows to fetch per round trip
* `jdbctogcs.output.mode` (Optional): Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
* `jdbctogcs.output.partitioncolumn` (Optional): Output partition column name

## Usage

```
$ python main.py --template JDBCTOGCS --help

usage: main.py --template JDBCTOGCS \
    --jdbctogcs.input.url JDBCTOGCS.INPUT.URL \
    --jdbctogcs.input.driver JDBCTOGCS.INPUT.DRIVER \
    --jdbctogcs.input.table JDBCTOGCS.INPUT.TABLE \
    --jdbctogcs.output.location JDBCTOGCS.OUTPUT.LOCATION \
    --jdbctogcs.output.format {avro,parquet,csv,json} \

optional arguments:
    -h, --help            show this help message and exit
    --jdbctogcs.input.partitioncolumn JDBCTOGCS.INPUT.PARTITIONCOLUMN \
    --jdbctogcs.input.lowerbound JDBCTOGCS.INPUT.LOWERBOUND \
    --jdbctogcs.input.upperbound JDBCTOGCS.INPUT.UPPERBOUND \
    --jdbctogcs.numpartitions JDBCTOGCS.NUMPARTITIONS \
    --jdbctogcs.input.fetchsize JDBCTOJDBC.INPUT.FETCHSIZE \
    --jdbctogcs.output.mode {overwrite,append,ignore,errorifexists} \
    --jdbctogcs.output.partitioncolumn JDBCTOGCS.OUTPUT.PARTITIONCOLUMN \
```

## General execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs staging location>
export SUBNET=<subnet>
export JARS="<gcs_path_to_jdbc_jar_files>/mysql-connector-java-8.0.29.jar,<gcs_path_to_jdbc_jar_files>/postgresql-42.2.6.jar,<gcs_path_to_jdbc_jar_files>/mssql-jdbc-6.4.0.jre8.jar"

./bin/start.sh \
-- --template=JDBCTOGCS \
--jdbctogcs.input.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>" \
--jdbctogcs.input.driver=<jdbc-driver-class-name> \
--jdbctogcs.input.table=<input table name or subquery with where clause filter> \
--jdbctogcs.input.partitioncolumn=<optional-partition-column-name> \
--jdbctogcs.input.lowerbound=<optional-partition-start-value>  \
--jdbctogcs.input.upperbound=<optional-partition-end-value>  \
--jdbctogcs.numpartitions=<optional-partition-number> \
--jdbctogcs.input.fetchsize=<optional-fetch-size> \
--jdbctogcs.output.location=<gcs-output-location> \
--jdbctogcs.output.mode=<optional-write-mode> \
--jdbctogcs.output.format=<output-write-format> \
--jdbctogcs.output.partitioncolumn=<optional-output-partition-column-name>
```

## Example execution:

```
export GCP_PROJECT=my-gcp-proj
export REGION=us-central1
export GCS_STAGING_LOCATION=gs://my-gcp-proj/staging
export SUBNET=projects/my-gcp-proj/regions/us-central1/subnetworks/default
export JARS="gs://my-gcp-proj/jars/mysql-connector-java-8.0.29.jar,gs://my-gcp-proj/jars/postgresql-42.2.6.jar,gs://my-gcp-proj/jars/mssql-jdbc-6.4.0.jre8.jar"
```
* MySQL to GCS
```
./bin/start.sh \
-- --template=JDBCTOGCS \
--jdbctogcs.input.url="jdbc:mysql://1.1.1.1:3306/mydb?user=root&password=password123" \
--jdbctogcs.input.driver="com.mysql.cj.jdbc.Driver" \
--jdbctogcs.input.table="(select * from employees where id <10) as employees" \
--jdbctogcs.input.partitioncolumn="id" \
--jdbctogcs.input.lowerbound="11" \
--jdbctogcs.input.upperbound="20" \
--jdbctogcs.numpartitions="4" \
--jdbctogcs.output.location="gs://output_bucket/output/" \
--jdbctogcs.output.mode="overwrite" \
--jdbctogcs.output.format="csv" \
--jdbctogcs.output.partitioncolumn="department_id"
```

* PostgreSQL to GCS
```
./bin/start.sh \
-- --template=JDBCTOGCS \
--jdbctogcs.input.url="jdbc:postgresql://1.1.1.1:5432/postgres?user=postgres&password=password123" \
--jdbctogcs.input.driver="org.postgresql.Driver" \
--jdbctogcs.input.table="(select * from employees) as employees" \
--jdbctogcs.input.partitioncolumn=id \
--jdbctogcs.input.lowerbound="11" \
--jdbctogcs.input.upperbound="20" \
--jdbctogcs.numpartitions="4" \
--jdbctogcs.output.location="gs://output_bucket/output/" \
--jdbctogcs.output.mode="overwrite" \
--jdbctogcs.output.format="csv" \
--jdbctogcs.output.partitioncolumn="department_id"
```

* Microsoft SQL Server to GCS
```
./bin/start.sh \
-- --template=JDBCTOGCS \
--jdbctogcs.input.url="jdbc:sqlserver://1.1.1.1:1433;databaseName=mydb;user=sqlserver;password=password123" \
--jdbctogcs.input.driver="com.microsoft.sqlserver.jdbc.SQLServerDriver" \
--jdbctogcs.input.table="employees" \
--jdbctogcs.input.partitioncolumn=id \
--jdbctogcs.input.lowerbound="11" \
--jdbctogcs.input.upperbound="20" \
--jdbctogcs.numpartitions="4" \
--jdbctogcs.output.location="gs://output_bucket/output/" \
--jdbctogcs.output.mode="overwrite" \
--jdbctogcs.output.format="csv" \
--jdbctogcs.output.partitioncolumn="department_id"
```

* Oracle to GCS
```
./bin/start.sh \
-- --template=JDBCTOGCS \
--jdbctogcs.input.url="jdbc:oracle:thin:@//1.1.1.1:1521/mydb?user=hr&password=password123" \
--jdbctogcs.input.driver="oracle.jdbc.driver.OracleDriver" \
--jdbctogcs.input.table="employees" \
--jdbctogcs.input.partitioncolumn=id \
--jdbctogcs.input.lowerbound="11" \
--jdbctogcs.input.upperbound="20" \
--jdbctogcs.numpartitions="4" \
--jdbctogcs.input.fetchsize="200" \
--jdbctogcs.output.location="gs://output_bucket/output/" \
--jdbctogcs.output.mode="overwrite" \
--jdbctogcs.output.format="csv" \
--jdbctogcs.output.partitioncolumn="department_id"
```

There are two optional properties as well with "JDBC to GCS" Template. Please find below the details :-

```
--templateProperty jdbctogcs.temp.view.name='temporary_view_name'
--templateProperty jdbctogcs.sql.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into GCS.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

While passing the properties for execution, either provide ```jdbctogcs.input.table``` or ```jdbctogcs.sql.query```. Passing both the properties would result in an error.

# 3. JDBC To BigQuery

Template for reading data from JDBC table and writing into files in Google Cloud BigQuery. It supports reading partition tables.

## Required JAR files

This template requires the JBDC jar files mentioned, and also the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## Arguments

* `jdbc.bigquery.input.url`: JDBC input URL
* `jdbc.bigquery.input.driver`: JDBC input driver name
* `jdbc.bigquery.input.table`: JDBC input table name
* `jdbc.bigquery.input.partitioncolumn` (Optional): JDBC input table partition column name
* `jdbc.bigquery.lowerbound` (Optional): JDBC input table partition column lower bound which is used to decide the partition stride
* `jdbc.bigquery.input.upperbound` (Optional): JDBC input table partition column upper bound which is used to decide the partition stride
* `jdbc.bigquery.numpartitions` (Optional): The maximum number of partitions that can be used for parallelism in table reading and writing. Same value will be used for both input and output jdbc connection. Default set to 10
* `jdbc.bigquery.input.fetchsize` (Optional): Determines how many rows to fetch per round trip
* `jdbc.bigquery.output.mode` (Optional): Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)

## Usage

```
$ python main.py --template JDBCTOBIGQUERY --help

usage: main.py [-h] --jdbc.bigquery.output.dataset
               JDBC.BIGQUERY.OUTPUT.DATASET
               --jdbc.bigquery.output.table
               JDBC.BIGQUERY.OUTPUT.TABLE
               --jdbc.bigquery.temp.bucket.name
               JDBC.BIGQUERY.TEMP.BUCKET.NAME
               --jdbc.bigquery.input.url JDBC.BIGQUERY.INPUT.URL
               --jdbc.bigquery.input.driver
               JDBC.BIGQUERY.INPUT.DRIVER
               --jdbc.bigquery.input.table JDBC.BIGQUERY.INPUT.TABLE
               [--jdbc.bigquery.input.partitioncolumn JDBC.BIGQUERY.INPUT.PARTITIONCOLUMN]
               [--jdbc.bigquery.input.lowerbound JDBC.BIGQUERY.INPUT.LOWERBOUND]
               [--jdbc.bigquery.input.upperbound JDBC.BIGQUERY.INPUT.UPPERBOUND]
               [--jdbc.bigquery.numpartitions JDBC.BIGQUERY.NUMPARTITIONS]
               [--jdbc.bigquery.input.fetchsize JDBC.BIGQUERY.INPUT.FETCHSIZE]
               [--jdbc.bigquery.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --jdbc.bigquery.output.dataset JDBC.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --jdbc.bigquery.output.table JDBC.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --jdbc.bigquery.temp.bucket.name JDBC.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --jdbc.bigquery.input.url JDBC.BIGQUERY.INPUT.URL
                        JDBC input URL
  --jdbc.bigquery.input.driver JDBC.BIGQUERY.INPUT.DRIVER
                        JDBC input driver name
  --jdbc.bigquery.input.table JDBC.BIGQUERY.INPUT.TABLE
                        JDBC input table name
  --jdbc.bigquery.input.partitioncolumn JDBC.BIGQUERY.INPUT.PARTITIONCOLUMN
                        JDBC input table partition column name
  --jdbc.bigquery.input.lowerbound JDBC.BIGQUERY.INPUT.LOWERBOUND
                        JDBC input table partition column lower
                        bound which is used to decide the partition
                        stride
  --jdbc.bigquery.input.upperbound JDBC.BIGQUERY.INPUT.UPPERBOUND
                        JDBC input table partition column upper
                        bound which is used to decide the partition
                        stride
  --jdbc.bigquery.numpartitions JDBC.BIGQUERY.NUMPARTITIONS
                        The maximum number of partitions that can be
                        used for parallelism in table reading and
                        writing. Default set to 10
  --jdbc.bigquery.input.fetchsize JDBC.BIGQUERY.INPUT.FETCHSIZE
                        Determines how many rows to fetch per round trip
  --jdbc.bigquery.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of:
                        append,overwrite,ignore,errorifexists)
                        (Defaults to append)

```

## General execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs staging location>
export SUBNET=<subnet>
export JARS="<gcs_path_to_jdbc_jar_files>/mysql-connector-java-8.0.29.jar,<gcs_path_to_jdbc_jar_files>/spark-bigquery-latest_2.12.jar"


./bin/start.sh \
-- --template=JDBCTOBIGQUERY \
--jdbc.bigquery.input.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>" \
--jdbc.bigquery.input.driver="<jdbc-driver-class-name>" \
--jdbc.bigquery.input.table="input table name or subquery with where clause filter" \
--jdbc.bigquery.output.mode="<append|overwrite|ignore|errorifexists>" \
--jdbc.bigquery.output.dataset="<bigquery-dataset-name>" \
--jdbc.bigquery.output.table="<bigquery-dataset-table>" \
--jdbc.bigquery.temp.bucket.name="<temp-bq-bucket-name>"

```
## Example execution:

```
export GCP_PROJECT=my-gcp-proj
export REGION=us-central1
export GCS_STAGING_LOCATION=gs://my-gcp-proj/staging
export SUBNET=projects/my-gcp-proj/regions/us-central1/subnetworks/default
export JARS="gs://my-gcp-proj/jars/mysql-connector-java-8.0.29.jar,gs://my-gcp-proj/jars/spark-bigquery-latest_2.12.jar"
```

* MySQL to BigQuery

```
./bin/start.sh \
-- --template=JDBCTOBIGQUERY \
--jdbc.bigquery.input.url="jdbc:mysql://1.1.1.1:3306/mydb?user=root&password=password123" \
--jdbc.bigquery.input.driver="com.mysql.cj.jdbc.Driver" \
--jdbc.bigquery.input.table="(select * from employees where id < 10) as employees" \
--jdbc.bigquery.input.partitioncolumn=id \
--jdbc.bigquery.input.lowerbound="11" \
--jdbc.bigquery.input.upperbound="20" \
--jdbc.bigquery.input.numpartitions="4" \
--jdbc.bigquery.output.mode="overwrite" \
--jdbc.bigquery.output.dataset="bq-dataset" \
--jdbc.bigquery.output.table="bq-table" \
--jdbc.bigquery.temp.bucket.name="temp-bq-bucket-name"
```

* PostgreSQL to BigQuery

```
./bin/start.sh \
-- --template=JDBCTOBIGQUERY \
--jdbc.bigquery.input.url="jdbc:postgresql://1.1.1.1:5432/postgres?user=postgres&password=password123" \
--jdbc.bigquery.input.driver="org.postgresql.Driver" \
--jdbc.bigquery.input.table="(select * from employees where id < 10) as employees" \
--jdbc.bigquery.input.partitioncolumn=id \
--jdbc.bigquery.input.lowerbound="11" \
--jdbc.bigquery.input.upperbound="20" \
--jdbc.bigquery.input.numpartitions="4" \
--jdbc.bigquery.output.mode="overwrite" \
--jdbc.bigquery.output.dataset="bq-dataset" \
--jdbc.bigquery.output.table="bq-table" \
--jdbc.bigquery.temp.bucket.name="temp-bq-bucket-name"
```

* Microsoft SQL Server to BigQuery
```
./bin/start.sh \
-- --template=JDBCTOBIGQUERY \
--jdbc.bigquery.input.url="jdbc:sqlserver://1.1.1.1:1433;databaseName=mydb;user=sqlserver;password=password123" \
--jdbc.bigquery.input.driver="com.microsoft.sqlserver.jdbc.SQLServerDriver" \
--jdbc.bigquery.input.table="employees" \
--jdbc.bigquery.input.partitioncolumn=id \
--jdbc.bigquery.input.lowerbound="11" \
--jdbc.bigquery.input.upperbound="20" \
--jdbc.bigquery.input.numpartitions="4" \
--jdbc.bigquery.output.mode="overwrite" \
--jdbc.bigquery.output.dataset="bq-dataset" \
--jdbc.bigquery.output.table="bq-table" \
--jdbc.bigquery.temp.bucket.name="temp-bq-bucket-name"
```

* Oracle to BigQuery
```
./bin/start.sh \
-- --template=JDBCTOBIGQUERY \
--jdbc.bigquery.input.url="jdbc:oracle:thin:@//1.1.1.1:1521/mydb?user=hr&password=password123" \
--jdbc.bigquery.input.driver="oracle.jdbc.driver.OracleDriver" \
--jdbc.bigquery.input.table="employees" \
--jdbc.bigquery.input.partitioncolumn=id \
--jdbc.bigquery.input.lowerbound="11" \
--jdbc.bigquery.input.upperbound="20" \
--jdbc.bigquery.input.numpartitions="4" \
--jdbc.bigquery.input.fetchsize="200" \
--jdbc.bigquery.output.mode="overwrite" \
--jdbc.bigquery.output.dataset="bq-dataset" \
--jdbc.bigquery.output.table="bq-table" \
--jdbc.bigquery.temp.bucket.name="temp-bq-bucket-name"
```
