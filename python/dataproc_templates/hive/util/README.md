# Hive DDL Extractor

This is a PySpark utility to extract DDLs of all the tables from a HIVE database using Hive Metastore. Users can use this utility to accelerate their Hive migration journey.  

It queries Hive Metastore via thrift to get DDLs for each table in the database, and outputs a single text file of semicolon-separated DDLs to a GCS bucket path, creating it with the following sub-path: "//hive_database/part-00000".

## Arguments

* `spark.hadoop.hive.metastore.uris`: Hive metastore URI
* `hive.ddl.extractor.input.database`: Hive database for input table  
* `hive.ddl.extractor.output.path`: Output GCS path. Ex: gs://bucket-name/path/to/directory  

## Optional Arguments

* `hive.ddl.consider.spark.tables`: Flag to extract DDL of Spark tables 
* `hive.ddl.translation.disposition`: Flag to remove location parameter from HIVE DDL to make DDL compatible with BQ Translation API

## Usage

```
$ python main.py --template HIVEDDLEXTRACTOR --help

usage: main.py --template HIVEDDLEXTRACTOR [-h] \
    --hive.ddl.extractor.input.database HIVE.DDL.EXTRACTOR.INPUT.DATABASE \
    --hive.ddl.extractor.output.path HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH

optional arguments:
  -h, --help            show this help message and exit
  --hive.ddl.extractor.input.database HIVE.DDL.EXTRACTOR.INPUT.DATABASE
                        Hive database for importing DDLs to GCS
  --hive.ddl.extractor.output.path HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH
                        GCS directory path e.g gs://bucket-name/path/to/directory
  --hive.ddl.extractor.input.database HIVE_DDL_CONSIDER_SPARK_TABLES
                        True / False
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
    --hive.ddl.extractor.input.database=True
```
