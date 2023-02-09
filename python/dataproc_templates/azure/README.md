# GCS To BigQuery

Template for reading files from Azure Blob Storage and writing them to a BigQuery table. It supports reading JSON, CSV, Parquet, Avro and Delta.IO formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `azure.storage.account`: Azure Storage Account  
* `azure.container.name`:  Azure Container Name
* `azure.sas`: Azure SAS Token
* `azure.bigquery.input.location`: Azure blob of the input files (format: `"wasbs://{azure.container.name}@{azure.storage.account}.blob.core.windows.net/{input_location}"`)
* `azure.bigquery.output.dataset`: BigQuery dataset for the output table
* `azure.bigquery.output.table`: BigQuery output table name
* `azure.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json)
* `azure.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `azure.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)

## Usage

```
$ python main.py --template AZURETOBQ --help

usage: main.py --template AZURETOBQ [-h] \
    --azure.bigquery.input.location AZURE.BIGQUERY.INPUT.LOCATION \
    --azure.bigquery.output.dataset AZURE.BIGQUERY.OUTPUT.DATASET \
    --azure.bigquery.output.table AZURE.BIGQUERY.OUTPUT.TABLE \
    --azure.bigquery.input.format {avro,parquet,csv,json} \
    --azure.bigquery.temp.bucket.name AZURE.BIGQUERY.TEMP.BUCKET.NAME \
    [--azure.bigquery.output.mode {overwrite,append,ignore,errorifexists}] \ 
    --azure.storage.account AZURE.STORAGE.ACCOUNT \
    --azure.container.name AZURE.CONTAINER.NAME \
    --azure.sas AZURE.SAS  

optional arguments:
  -h, --help            show this help message and exit
  --azure.bigquery.input.location AZURE.BIGQUERY.INPUT.LOCATION
                        AZURE location of the input files
  --azure.bigquery.output.dataset AZURE.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --azure.bigquery.output.table AZURE.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --azure.bigquery.input.format {avro,parquet,csv,json}
                        Input file format (one of: avro,parquet,csv,json)
  --azure.bigquery.temp.bucket.name AZURE.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --azure.bigquery.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --azure.storage.account AZURE.STORAGE.ACCOUNT 
                        Azure storage account
  --azure.container.name AZURE.CONTAINER.NAME 
                        Azure container name
  --azure.sas AZURE.SAS 
                        Azure SAS token      
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

***For delta.io input format you need to provide delta dependencies `delta-core.jar` for example dataproc serverless (PySpark) Runtime version 1.0 (Spark 3.2, Java 11, Scala 2.12) requires delta version 1.1.0 you can check the delta and spark dependencies [here](https://docs.delta.io/latest/releases.html)***
## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
## for delta.io format 
# export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar, gs://{jars_bucket}/delta-core_2.12-1.1.0.jar" 

./bin/start.sh \
-- --template=AZURETOBQ \
    --azure.bigquery.input.format="<json|csv|parquet|avro>" \
    --azure.bigquery.input.location="wasbs://{azure.container.name}@{azure.storage.account}.blob.core.windows.net/{input_location}" \
    --azure.bigquery.output.dataset="<dataset>" \
    --azure.bigquery.output.table="<table>" \
    --azure.bigquery.output.mode=<append|overwrite|ignore|errorifexists>\
    --azure.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
    --azure.storage.account AZURE.STORAGE.ACCOUNT \
    --azure.container.name AZURE.CONTAINER.NAME \
    --azure.sas AZURE.SAS 
```
