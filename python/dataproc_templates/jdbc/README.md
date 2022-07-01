# 1. JDBC To JDBC

Template for reading data from JDBC table and writing them to a JDBC table. It supports reading partition tabels and write into partitioned or non-partitioned tables.

## Arguments

* `jdbctojdbc.input.url`: JDBC input URL
* `jdbctojdbc.input.driver`: JDBC input driver name
* `jdbctojdbc.input.table`: JDBC input table name
* `jdbctojdbc.input.partitioncolumn`: JDBC input table partition column name
* `jdbctojdbc.input.lowerbound`: JDBC input table partition column lower bound which is used to decide the partition stride
* `jdbctojdbc.input.upperbound`: JDBC input table partition column upper bound which is used to decide the partition stride
* `jdbctojdbc.numpartitions`: The maximum number of partitions that can be used for parallelism in table reading and writing. Same value will be used for both input and output jdbc connection. Default set to 10
* `jdbctojdbc.output.url`: JDBC output url
* `jdbctojdbc.output.driver`: JDBC output driver name
* `jdbctojdbc.output.table`: JDBC output table name
* `jdbctojdbc.output.create_table.option`: This option allows setting of database-specific table and partition options when creating a output table
* `jdbctojdbc.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `jdbctojdbc.output.batch.size`: JDBC output batch size. Default set to 1000

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
    --jdbctojdbc.output.create_table.option JDBCTOJDBC.OUTPUT.CREATE_TABLE.OPTION \
    --jdbctojdbc.output.mode {overwrite,append,ignore,errorifexists} \
    --jdbctojdbc.output.batch.size JDBCTOJDBC.OUTPUT.BATCH.SIZE \
```

## Required JAR files

This template requires the JDBC jar file to be available in the Dataproc cluster.
User has to download the required jar file and host it inside a GCS Bucket, so that it could be referred during the execution of code.

wget Command to download JDBC MySQL jar file is as follows :-

```
wget http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.30.tar.gz. -O /tmp/mysql-connector.tar.gz 

```

Once the jar file gets downloaded, please upload the file into a GCS Bucket and export the below variable

```
export JARS=<gcs-bucket-location-containing-jar-file> 
```

## Other important properties

* Following is example JDBC URL for mysql database

```
jdbctojdbc.input.url=jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>
```

* You can either specify the source table name or have SQL query within double quotes. Example,

```
jdbctojdbc.input.table="employees"
jdbctojdbc.input.table="(select * from employees where dept_id>10) as employees"
```

* partitionColumn, lowerBound, upperBound and numPartitions must be used together. If one is specified then all needs to be specified.

* You can specify the target table properties such as partition column using below property. This is useful when target table is not present or when write mode=overwrite and you need the target table to be created as partitioned table.

```
jdbctojdbc.output.create_table.option="PARTITION BY RANGE(id)  (PARTITION p0 VALUES LESS THAN (5),PARTITION p1 VALUES LESS THAN (10),PARTITION p2 VALUES LESS THAN (15),PARTITION p3 VALUES LESS THAN MAXVALUE)"
```

* Additional execution details [refer spark jdbc doc](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

## Example execution: 

```
export GCP_PROJECT=<gcp-project-id> \
export REGION=<region>  \
export GCS_STAGING_LOCATION=<gcs staging location> \
export SUBNET=<subnet>   \
export JARS=<gcs_path_to_jdbc_jar_files>

./bin/start.sh \
-- --template=JDBCTOJDBC \
--jdbctojdbc.input.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>" \
--jdbctojdbc.input.driver=<jdbc-driver-class-name> \
--jdbctojdbc.input.table=<input table name or subquery with where clause filter> \
--jdbctojdbc.input.partitioncolumn=<optional-partition-column-name> \
--jdbctojdbc.input.lowerbound=<optional-partition-start-value>  \
--jdbctojdbc.input.upperbound=<optional-partition-end-value>  \
--jdbctojdbc.numpartitions=<optional-partition-number> \
--jdbctojdbc.output.url="jdbc:mysql://<hostname>:<port>/<dbname>?user=<username>&password=<password>" \
--jdbctojdbc.output.driver=<jdbc-driver-class-name> \
--jdbctojdbc.output.table=<output table name> \
--jdbctojdbc.output.create_table.option=<optional_output-table-properties> \
--jdbctojdbc.output.mode="<optional_write-mode> \
--jdbctojdbc.output.batch.size=<optional_batch-size>
```
