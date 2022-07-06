# Dataproc Serverless: Databases To GCS

Template for exporting a Database table to files in Google Cloud Storage. It supports writing JSON, CSV, Parquet and Avro formats for Postgresql, MySql, MS SQL Server (GCP and onprem).

## Arguments

* `db.gcs.source.jdbc.url`: JDBC connection string
* `db.gcs.jdbc.driver`: JDBC driver name
* `db.gcs.source.table`: Source database's table name whose data needs to be migrated to GCS
* `db.gcs.destination.location`: GCS location of the output files (format: `gs://bucket/...`) 
* `db.gcs.output.format`: Output file format (one of: avro,parquet,csv,json) (Defaults to parquet)
* `db.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to overwrite)

## Usage

```
$ python main.py --template DATABASESTOGCS --help

usage: main.py [-h] \
  --db.gcs.source.jdbc.url DB.GCS.SOURCE.JDBC.URL \
  --db.gcs.jdbc.driver DB.GCS.JDBC.DRIVER \
  --db.gcs.source.table DB.GCS.SOURCE.TABLE \ 
  --db.gcs.destination.location DB.GCS.DESTINATION.LOCATION \
  [--db.gcs.output.format {avro,parquet,csv,json}] \
  [--db.gcs.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help    show this help message and exit
  --gcs.bigquery.input.location GCS.BIGQUERY.INPUT.LOCATION
                GCS location of the input files
  --db.gcs.source.jdbc.url DB.GCS.SOURCE.JDBC.URL
                JDBC connection URL consisting of username and 
                password as well
  --db.gcs.jdbc.driver DB.GCS.JDBC.DRIVER
                JDBC driver name
  --db.gcs.source.table DB.GCS.SOURCE.TABLE
                Table's name whose data is to be migrated to GCS
  --db.gcs.destination.location DB.GCS.DESTINATION.LOCATION
                GCS location for output files
  --db.gcs.output.format {avro,parquet,csv,json}
                Output file format (one of: avro,parquet,csv,json)
                (Defaults to parquet)
  --db.gcs.output.mode {overwrite,append,ignore,errorifexists}
                Output write mode (one of:
                append,overwrite,ignore,errorifexists) 
                (Defaults to overwrite) 
```

## Required JAR files

This template requires the JDBBC connection JAR corresponsing to the selected source database, to be available in the Dataproc cluster. Steps to make the JARS available to dataproc:


* User has to download the required jar file and host it inside a GCS Bucket, so that it could be referred during the execution of code.
* Once the jar file gets downloaded, please upload the file into a GCS Bucket.

- **MySQL connector JAR:**
  - gs://<your_bucket_to_store_dependencies>/mysql-connector-java-8.0.29.jar`
    - Download it using ``` wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.29/mysql-connector-java-8.0.29.jar```

- **PostgreSQL connector JAR:**
  - gs://<your_bucket_to_store_dependencies>/postgresql-42.4.0.jar`
    - Download it using ``` wget https://jdbc.postgresql.org/download/postgresql-42.4.0.jar```

- **MS SQL Server connector JAR:**
  - gs://<your_bucket_to_store_dependencies>/mssql-jdbc-9.4.1.jre8.jar`
    - Download it using ``` wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.4.1.jre8/mssql-jdbc-9.4.1.jre8.jar```

## Example submission

```
export GCP_PROJECT=<project_id>
export JARS="gs://bucket_to_store_dependencies/jdbc_connection.jar"
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export REGION=<region>

./bin/start.sh \
-- --template=DATABASESTOGCS \
	--db.gcs.source.jdbc.url=<jdbc url> \
	--db.gcs.jdbc.driver=<jdbc driver> \
	--db.gcs.source.table=<table name | query > \
    --db.gcs.destination.location=<gs://bucket/path>
	--db.gcs.output.format=<csv|parquet|avro|json> \
	--db.gcs.output.mode=<overwrite|append|ignore|errorifexists>
```
