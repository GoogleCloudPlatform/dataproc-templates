# Hive DDL Extractor

This is a PySpark utility to extract DDLs of all the tables from a HIVE database using Hive Metastore. Users can use this utility to accelerate their Hive migration journey.  

It queries Hive Metastore via thrift to get DDLs for each table in the database, and outputs a single text file of semicolon-separated DDLs to a GCS bucket path, creating it with the following sub-path: "//hive_database/part-00000".

## Arguments

* `spark.hadoop.hive.metastore.uris`: Hive metastore URI
* `hive.ddl.extractor.input.database`: Hive database for input table  
* `hive.ddl.extractor.output.path`: Output GCS path. Ex: gs://bucket-name/path/to/directory  

## Optional Arguments

* `hive.ddl.consider.spark.tables`: Flag to extract DDL of Spark tables 
* `hive.ddl.translation.disposition`: Flag to remove location parameter from HIVE DDL to make DDL compatible with BigQuery SQL translator (the translated query will be a native BQ table and not an external one

## Usage

```
$ python main.py --template HIVEDDLEXTRACTOR --help

usage: main.py [-h] --hive.ddl.extractor.input.database HIVE.DDL.EXTRACTOR.INPUT.DATABASE --hive.ddl.extractor.output.path HIVE.DDL.EXTRACTOR.OUTPUT.PATH
               [--hive.ddl.consider.spark.tables HIVE.DDL.CONSIDER.SPARK.TABLES] [--hive.ddl.translation.disposition HIVE.DDL.TRANSLATION.DISPOSITION]

optional arguments:
  -h, --help            show this help message and exit
  --hive.ddl.extractor.input.database HIVE.DDL.EXTRACTOR.INPUT.DATABASE
                        Hive database for importing data to BigQuery
  --hive.ddl.extractor.output.path HIVE.DDL.EXTRACTOR.OUTPUT.PATH
                        GCS output path
  --hive.ddl.consider.spark.tables HIVE.DDL.CONSIDER.SPARK.TABLES
                        Flag to extract DDL of Spark tables
  --hive.ddl.translation.disposition HIVE.DDL.TRANSLATION.DISPOSITION
                        Remove location parameter from HIVE DDL if set to TRUE, to be compatible with BigQuery SQL translator (the translated query will be a native BQ table and not an external one.
```

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gs://gcs-staging-bucket-folder>
export SUBNET=<subnet>
./bin/start.sh \
    --properties=spark.hadoop.hive.metastore.uris=thrift://<hostname-or-ip>:9083 \
    -- --template=HIVEDDLEXTRACTOR \
    --hive.ddl.extractor.input.database="<database>" \
    --hive.ddl.extractor.output.path="<gs://bucket-name/path/to/directory>"
    --hive.ddl.consider.spark.tables=false \
    --hive.ddl.translation.disposition=false
```
