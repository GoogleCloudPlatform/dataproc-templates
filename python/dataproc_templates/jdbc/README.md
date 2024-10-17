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
* **Note:
JDBC Connections now allow use of secrets created in Cloud Secret Manager. Please check the examples in respective sections.**

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
* `jdbctojdbc.input.url.secret`: JDBC input URL secret. Pass the secret name as created in Cloud Secret Manager
> Note: Please provide only one of the above two properties (`jdbctojdbc.input.url` or `jdbctojdbc.input.url.secret`)
* `jdbctojdbc.input.driver`: JDBC input driver name
* `jdbctojdbc.input.table`: JDBC input table name
* `jdbctojdbc.output.url`: JDBC output url. When the JDBC target is PostgreSQL it is recommended to include the connection parameter reWriteBatchedInserts=true in the URL to provide a significant performance improvement over the default setting. OR provide secret name enclosed inside { }
* `jdbctojdbc.output.url.secret`: JDBC output URL secret. Pass the secret name as created in Cloud Secret Manager.
> Note: Please provide only one of the above two properties (`jdbctojdbc.output.url` or `jdbctojdbc.output.url.secret`)
* `jdbctojdbc.output.driver`: JDBC output driver name
* `jdbctojdbc.output.table`: JDBC output table name
* `jdbctojdbc.input.partitioncolumn` (Optional): JDBC input table partition column name
* `jdbctojdbc.input.lowerbound` (Optional): JDBC input table partition column lower bound which is used to decide the partition stride
* `jdbctojdbc.input.upperbound` (Optional): JDBC input table partition column upper bound which is used to decide the partition stride
* `jdbctojdbc.numpartitions` (Optional): The maximum number of partitions that can be used for parallelism in table reading and writing. Same value will be used for both input and output jdbc connection. Default set to 10
* `jdbctojdbc.input.fetchsize` (Optional): Determines how many rows to fetch per round trip
* `jdbctojdbc.input.sessioninitstatement` (Optional): Custom SQL statement to execute in each reader database session
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
    --jdbctojdbc.input.url.secret JDBCTOJDBC.INPUT.URL.SECRET \
    --jdbctojdbc.input.partitioncolumn JDBCTOJDBC.INPUT.PARTITIONCOLUMN \
    --jdbctojdbc.input.lowerbound JDBCTOJDBC.INPUT.LOWERBOUND \
    --jdbctojdbc.input.upperbound JDBCTOJDBC.INPUT.UPPERBOUND \
    --jdbctojdbc.numpartitions JDBCTOJDBC.NUMPARTITIONS \
    --jdbctojdbc.input.fetchsize JDBCTOJDBC.INPUT.FETCHSIZE \
    --jdbctojdbc.input.sessioninitstatement JDBCTOJDBC.INPUT.SESSIONINITSTATEMENT \
    --jdbctojdbc.output.url.secret JDBCTOJDBC.OUTPUT.URL.SECRET \
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
--jdbctojdbc.input.sessioninitstatement=<optional-SQL-statement> \
--jdbctojdbc.output.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>" \
--jdbctojdbc.output.driver=<jdbc-driver-class-name> \
--jdbctojdbc.output.table=<output-table-name> \
--jdbctojdbc.output.create_table.option=<optional-output-table-properties> \
--jdbctojdbc.output.mode=<optional-write-mode> \
--jdbctojdbc.output.batch.size=<optional-batch-size>

WITH SECRET

./bin/start.sh \
-- --template=JDBCTOJDBC \
--jdbctojdbc.input.url.secret="<jdbc-input-url-secret-name>" \
--jdbctojdbc.input.driver=<jdbc-driver-class-name> \
--jdbctojdbc.input.table=<input table name or subquery with where clause filter> \
--jdbctojdbc.input.partitioncolumn=<optional-partition-column-name> \
--jdbctojdbc.input.lowerbound=<optional-partition-start-value>  \
--jdbctojdbc.input.upperbound=<optional-partition-end-value>  \
--jdbctojdbc.numpartitions=<optional-partition-number> \
--jdbctojdbc.input.fetchsize=<optional-fetch-size> \
--jdbctojdbc.input.sessioninitstatement=<optional-SQL-statement> \
--jdbctojdbc.output.url.secret="<jdbc-output-url-secret-name>" \
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

WITH SECRET

./bin/start.sh \
-- --template=JDBCTOJDBC \
--jdbctojdbc.input.url.secret="jdbctojdbcurlsecret" \
--jdbctojdbc.input.driver="com.mysql.cj.jdbc.Driver" \
--jdbctojdbc.input.table="(select * from employees where id <10) as employees" \
--jdbctojdbc.input.partitioncolumn=id \
--jdbctojdbc.input.lowerbound="1" \
--jdbctojdbc.input.upperbound="10" \
--jdbctojdbc.numpartitions="4" \
--jdbctojdbc.output.url.secret="jdbctojdbcoutputurlsecret" \
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
--jdbctojdbc.output.url="jdbc:postgresql://1.1.1.1:5432/postgres?user=postgres&password=password123&reWriteBatchedInserts=True" \
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
--jdbctojdbc.output.url="jdbc:postgresql://1.1.1.1:5432/postgres?user=postgres&password=password123&reWriteBatchedInserts=True" \
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
--jdbctojdbc.input.sessioninitstatement="BEGIN DBMS_APPLICATION_INFO.SET_MODULE('Dataproc Templates','JDBCTOJDBC'); END;" \
--jdbctojdbc.output.url="jdbc:postgresql://1.1.1.1:5432/postgres?user=postgres&password=password123&reWriteBatchedInserts=True" \
--jdbctojdbc.output.driver="org.postgresql.Driver" \
--jdbctojdbc.output.table="employees_out" \
--jdbctojdbc.output.mode="overwrite" \
--jdbctojdbc.output.batch.size="1000"
```

There are two optional properties as well with "JDBC to JDBC" Template. Please find below the details :-

```
--templateProperty jdbctojdbc.temp.view.name='temporary_view_name'
--templateProperty jdbctojdbc.sql.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into JDBC.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"


# 2. JDBC To Cloud Storage

Template for reading data from JDBC table and writing into files in Google Cloud Storage. It supports reading partition tabels and supports writing in JSON, CSV, Parquet and Avro formats.

## Arguments
* `jdbctogcs.input.url`: JDBC input URL 
* `jdbctogcs.input.url.secret`: JDBC input URL secret. Pass the secret name as created in Cloud Secret Manager.
> Note: Please provide only one of the above two properties (`jdbctogcs.input.url` or `jdbctogcs.input.url.secret`)
* `jdbctogcs.input.driver`: JDBC input driver name
* `jdbctogcs.input.table`: JDBC input table name
* `jdbctogcs.input.sql.query`: JDBC input SQL query
* `jdbctogcs.output.location`: Cloud Storage location for output files (format: `gs://BUCKET/...`)
* `jdbctogcs.output.format`: Output file format (one of: avro,parquet,csv,json)
* `jdbctogcs.input.partitioncolumn` (Optional): JDBC input table partition column name
* `jdbctogcs.input.lowerbound` (Optional): JDBC input table partition column lower bound which is used to decide the partition stride
* `jdbctogcs.input.upperbound` (Optional): JDBC input table partition column upper bound which is used to decide the partition stride
* `jdbctogcs.numpartitions` (Optional): The maximum number of partitions that can be used for parallelism in table reading and writing. Same value will be used for both input and output jdbc connection. Default set to 10
* `jdbctogcs.input.fetchsize` (Optional): Determines how many rows to fetch per round trip
* `jdbctogcs.input.sessioninitstatement` (Optional): Custom SQL statement to execute in each reader database session
* `jdbctogcs.output.mode` (Optional): Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
* `jdbctogcs.output.partitioncolumn` (Optional): Output partition column name
#### Optional Arguments
* `jdbctogcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `jdbctogcs.output.compression`: None
* `jdbctogcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
* `jdbctogcs.output.emptyvalue`: Sets the string representation of an empty value
* `jdbctogcs.output.encoding`: Decodes the CSV files by the given encoding type
* `jdbctogcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `jdbctogcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
* `jdbctogcs.output.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `jdbctogcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `jdbctogcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `jdbctogcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `jdbctogcs.output.nullvalue`: Sets the string representation of a null value
* `jdbctogcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `jdbctogcs.output.quoteall`: None
* `jdbctogcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `jdbctogcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `jdbctogcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format

## Usage

```
$ python main.py --template JDBCTOGCS --help

usage: main.py [-h]
               --jdbctogcs.input.url JDBCTOGCS.INPUT.URL
               --jdbctogcs.input.driver JDBCTOGCS.INPUT.DRIVER
               [--jdbctogcs.input.table JDBCTOGCS.INPUT.TABLE]
               [--jdbctogcs.input.sql.query JDBCTOGCS.INPUT.SQL.QUERY]
               [--jdbctogcs.input.partitioncolumn JDBCTOGCS.INPUT.PARTITIONCOLUMN]
               [--jdbctogcs.input.lowerbound JDBCTOGCS.INPUT.LOWERBOUND]
               [--jdbctogcs.input.upperbound JDBCTOGCS.INPUT.UPPERBOUND]
               [--jdbctogcs.numpartitions JDBCTOGCS.NUMPARTITIONS]
               [--jdbctogcs.input.fetchsize JDBCTOGCS.INPUT.FETCHSIZE]
               [--jdbctogcs.input.sessioninitstatement JDBCTOGCS.INPUT.SESSIONINITSTATEMENT]
               --jdbctogcs.output.location JDBCTOGCS.OUTPUT.LOCATION --jdbctogcs.output.format {avro,parquet,csv,json}
               [--jdbctogcs.output.mode {overwrite,append,ignore,errorifexists}]
               [--jdbctogcs.output.partitioncolumn JDBCTOGCS.OUTPUT.PARTITIONCOLUMN]
               [--jdbctogcs.temp.view.name JDBCTOGCS.TEMP.VIEW.NAME]
               [--jdbctogcs.temp.sql.query JDBCTOGCS.TEMP.SQL.QUERY]
               [--jdbctogcs.output.chartoescapequoteescaping JDBCTOGCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--jdbctogcs.output.compression JDBCTOGCS.OUTPUT.COMPRESSION]
               [--jdbctogcs.output.dateformat JDBCTOGCS.OUTPUT.DATEFORMAT]
               [--jdbctogcs.output.emptyvalue JDBCTOGCS.OUTPUT.EMPTYVALUE]
               [--jdbctogcs.output.encoding JDBCTOGCS.OUTPUT.ENCODING]
               [--jdbctogcs.output.escape JDBCTOGCS.OUTPUT.ESCAPE]
               [--jdbctogcs.output.escapequotes JDBCTOGCS.OUTPUT.ESCAPEQUOTES]
               [--jdbctogcs.output.header JDBCTOGCS.OUTPUT.HEADER]
               [--jdbctogcs.output.ignoreleadingwhitespace JDBCTOGCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--jdbctogcs.output.ignoretrailingwhitespace JDBCTOGCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--jdbctogcs.output.linesep JDBCTOGCS.OUTPUT.LINESEP]
               [--jdbctogcs.output.nullvalue JDBCTOGCS.OUTPUT.NULLVALUE]
               [--jdbctogcs.output.quote JDBCTOGCS.OUTPUT.QUOTE]
               [--jdbctogcs.output.quoteall JDBCTOGCS.OUTPUT.QUOTEALL]
               [--jdbctogcs.output.sep JDBCTOGCS.OUTPUT.SEP]
               [--jdbctogcs.output.timestampformat JDBCTOGCS.OUTPUT.TIMESTAMPFORMAT]
               [--jdbctogcs.output.timestampntzformat JDBCTOGCS.OUTPUT.TIMESTAMPNTZFORMAT]

options:
  -h, --help            show this help message and exit
  --jdbctogcs.input.url JDBCTOGCS.INPUT.URL
                        JDBC input URL
  --jdbctogcs.input.url.secret JDBCTOGCS.INPUT.URL.SECRET
                        JDBC input URL secret. Pass the secret name as created in Cloud Secret Manager.                  
  --jdbctogcs.input.driver JDBCTOGCS.INPUT.DRIVER
                        JDBC input driver name
  --jdbctogcs.input.table JDBCTOGCS.INPUT.TABLE
                        JDBC input table name
  --jdbctogcs.input.sql.query JDBCTOGCS.INPUT.SQL.QUERY
                        JDBC input SQL query
  --jdbctogcs.input.partitioncolumn JDBCTOGCS.INPUT.PARTITIONCOLUMN
                        JDBC input table partition column name
  --jdbctogcs.input.lowerbound JDBCTOGCS.INPUT.LOWERBOUND
                        JDBC input table partition column lower bound which is used to decide the partition stride
  --jdbctogcs.input.upperbound JDBCTOGCS.INPUT.UPPERBOUND
                        JDBC input table partition column upper bound which is used to decide the partition stride
  --jdbctogcs.numpartitions JDBCTOGCS.NUMPARTITIONS
                        The maximum number of partitions that can be used for parallelism in table reading and writing. Default set to 10
  --jdbctogcs.input.fetchsize JDBCTOGCS.INPUT.FETCHSIZE
                        Determines how many rows to fetch per round trip
  --jdbctogcs.input.sessioninitstatement JDBCTOGCS.INPUT.SESSIONINITSTATEMENT
                        Custom SQL statement to execute in each reader database session
  --jdbctogcs.output.location JDBCTOGCS.OUTPUT.LOCATION
                        Cloud Storage location for output files
  --jdbctogcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --jdbctogcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --jdbctogcs.output.partitioncolumn JDBCTOGCS.OUTPUT.PARTITIONCOLUMN
                        Cloud Storage partition column name
  --jdbctogcs.temp.view.name JDBCTOGCS.TEMP.VIEW.NAME
                        Temp view name for creating a spark sql view on source data. This name has to match with the table name that will be used in the SQL query
  --jdbctogcs.temp.sql.query JDBCTOGCS.TEMP.SQL.QUERY
                        SQL query for data transformation. This must use the temp view name as the table to query from.
  --jdbctogcs.output.chartoescapequoteescaping JDBCTOGCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --jdbctogcs.output.compression JDBCTOGCS.OUTPUT.COMPRESSION
  --jdbctogcs.output.dateformat JDBCTOGCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --jdbctogcs.output.emptyvalue JDBCTOGCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --jdbctogcs.output.encoding JDBCTOGCS.OUTPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --jdbctogcs.output.escape JDBCTOGCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --jdbctogcs.output.escapequotes JDBCTOGCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --jdbctogcs.output.header JDBCTOGCS.OUTPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --jdbctogcs.output.ignoreleadingwhitespace JDBCTOGCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --jdbctogcs.output.ignoretrailingwhitespace JDBCTOGCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --jdbctogcs.output.linesep JDBCTOGCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --jdbctogcs.output.nullvalue JDBCTOGCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --jdbctogcs.output.quote JDBCTOGCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --jdbctogcs.output.quoteall JDBCTOGCS.OUTPUT.QUOTEALL
  --jdbctogcs.output.sep JDBCTOGCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --jdbctogcs.output.timestampformat JDBCTOGCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --jdbctogcs.output.timestampntzformat JDBCTOGCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
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
--jdbctogcs.input.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>" OR "{jdbc_url_secret_name}" \
--jdbctogcs.input.driver=<jdbc-driver-class-name> \
--jdbctogcs.input.table=<input table name or subquery with where clause filter> \
--jdbctogcs.input.partitioncolumn=<optional-partition-column-name> \
--jdbctogcs.input.lowerbound=<optional-partition-start-value>  \
--jdbctogcs.input.upperbound=<optional-partition-end-value>  \
--jdbctogcs.numpartitions=<optional-partition-number> \
--jdbctogcs.input.fetchsize=<optional-fetch-size> \
--jdbctogcs.input.sessioninitstatement=<optional-SQL-statement> \
--jdbctogcs.output.location=<gcs-output-location> \
--jdbctogcs.output.mode=<optional-write-mode> \
--jdbctogcs.output.format=<output-write-format> \
--jdbctogcs.output.partitioncolumn=<optional-output-partition-column-name>

WITH SECRET 

./bin/start.sh \
-- --template=JDBCTOGCS \
--jdbctogcs.input.url.secret="<jdbc-input-url-secret-name>" \
--jdbctogcs.input.driver=<jdbc-driver-class-name> \
--jdbctogcs.input.table=<input table name or subquery with where clause filter> \
--jdbctogcs.input.partitioncolumn=<optional-partition-column-name> \
--jdbctogcs.input.lowerbound=<optional-partition-start-value>  \
--jdbctogcs.input.upperbound=<optional-partition-end-value>  \
--jdbctogcs.numpartitions=<optional-partition-number> \
--jdbctogcs.input.fetchsize=<optional-fetch-size> \
--jdbctogcs.input.sessioninitstatement=<optional-SQL-statement> \
--jdbctogcs.output.location=<gcs-output-location> \
--jdbctogcs.output.mode=<optional-write-mode> \
--jdbctogcs.output.format=<output-write-format> \
--jdbctogcs.output.partitioncolumn=<optional-output-partition-column-name>
```

Instead of input table name, an input SQL query can also be passed. Example,
```
--jdbctogcs.input.sql.query="select * from table"
```
Note: While passing the properties for execution, either provide ```jdbctogcs.input.table``` or ```jdbctogcs.input.sql.query```. Passing both the properties would result in an error.

## Example execution:

```
export GCP_PROJECT=my-gcp-proj
export REGION=us-central1
export GCS_STAGING_LOCATION=gs://my-gcp-proj/staging
export SUBNET=projects/my-gcp-proj/regions/us-central1/subnetworks/default
export JARS="gs://my-gcp-proj/jars/mysql-connector-java-8.0.29.jar,gs://my-gcp-proj/jars/postgresql-42.2.6.jar,gs://my-gcp-proj/jars/mssql-jdbc-6.4.0.jre8.jar"
```
* MySQL to Cloud Storage
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

WITH SECRET

./bin/start.sh \
-- --template=JDBCTOGCS \
--jdbctogcs.input.url.secret="jdbctogcsinputurl" \
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

* PostgreSQL to Cloud Storage
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

* Microsoft SQL Server to Cloud Storage
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

* Oracle to Cloud Storage
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
--jdbctogcs.input.sessioninitstatement="BEGIN DBMS_APPLICATION_INFO.SET_MODULE('Dataproc Templates','JDBCTOGCS'); END;" \
--jdbctogcs.output.location="gs://output_bucket/output/" \
--jdbctogcs.output.mode="overwrite" \
--jdbctogcs.output.format="csv" \
--jdbctogcs.output.partitioncolumn="department_id"
```

There are two optional properties as well with "JDBC to Cloud Storage" Template. Please find below the details :-

```
--templateProperty jdbctogcs.temp.view.name='temporary_view_name'
--templateProperty jdbctogcs.temp.sql.query='select * from global_temp.temporary_view_name'
```
These properties are responsible for applying some spark sql transformations before loading data into Cloud Storage.
The only thing needs to keep in mind is that, the name of the Spark temporary view and the name of table in the query should match exactly. Otherwise, there would be an error as:- "Table or view not found:"

# 3. JDBC To BigQuery

Template for reading data from JDBC table and writing into files in Google Cloud BigQuery. It supports reading partition tables.

## Arguments

* `jdbc.bigquery.input.url`: JDBC input URL
* `jdbc.bigquery.input.url.secret`: JDBC input URL secret. Pass the secret name as created in Cloud Secret Manager.
> Note: Please provide only one of the above two properties (`jdbc.bigquery.input.url` or `jdbc.bigquery.input.url.secret`)
* `jdbc.bigquery.input.driver`: JDBC input driver name
* `jdbc.bigquery.input.table`: JDBC input table name
* `jdbc.bigquery.input.partitioncolumn` (Optional): JDBC input table partition column name
* `jdbc.bigquery.lowerbound` (Optional): JDBC input table partition column lower bound which is used to decide the partition stride
* `jdbc.bigquery.input.upperbound` (Optional): JDBC input table partition column upper bound which is used to decide the partition stride
* `jdbc.bigquery.numpartitions` (Optional): The maximum number of partitions that can be used for parallelism in table reading and writing. Same value will be used for both input and output jdbc connection. Default set to 10
* `jdbc.bigquery.input.fetchsize` (Optional): Determines how many rows to fetch per round trip
* `jdbc.bigquery.input.sessioninitstatement` (Optional): Custom SQL statement to execute in each reader database session
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
               [--jdbc.bigquery.input.sessioninitstatement JDBC.BIGQUERY.INPUT.SESSIONINITSTATEMENT]
               [--jdbc.bigquery.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --jdbc.bigquery.input.url.secret JDBC.BIGQUERY.INPUT.URL.SECRET
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
  --jdbc.bigquery.input.sessioninitstatement JDBC.BIGQUERY.INPUT.SESSIONINITSTATEMENT
                        Custom SQL statement to execute in each reader database session
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
export JARS="<gcs_path_to_jdbc_jar_files>/mysql-connector-java-8.0.29.jar"


./bin/start.sh \
-- --template=JDBCTOBIGQUERY \
--jdbc.bigquery.input.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>" OR "{jdbc_url_secret_name}" \
--jdbc.bigquery.input.driver="<jdbc-driver-class-name>" \
--jdbc.bigquery.input.table="input table name or subquery with where clause filter" \
--jdbc.bigquery.output.mode="<append|overwrite|ignore|errorifexists>" \
--jdbc.bigquery.output.dataset="<bigquery-dataset-name>" \
--jdbc.bigquery.output.table="<bigquery-dataset-table>" \
--jdbc.bigquery.temp.bucket.name="<temp-bq-bucket-name>"

WITH SECRET 

./bin/start.sh \
-- --template=JDBCTOBIGQUERY \
--jdbc.bigquery.input.url.secret="<jdbc-input-url-secret-name>" \
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
export JARS="gs://my-gcp-proj/jars/mysql-connector-java-8.0.29.jar"
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

WITH SECRET 

./bin/start.sh \
-- --template=JDBCTOBIGQUERY \
--jdbc.bigquery.input.url.secret="jdbctobqinputurl" \
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
--jdbc.bigquery.input.sessioninitstatement="BEGIN DBMS_APPLICATION_INFO.SET_MODULE('Dataproc Templates','JDBCTOBIGQUERY'); END;" \
--jdbc.bigquery.output.mode="overwrite" \
--jdbc.bigquery.output.dataset="bq-dataset" \
--jdbc.bigquery.output.table="bq-table" \
--jdbc.bigquery.temp.bucket.name="temp-bq-bucket-name"
```
