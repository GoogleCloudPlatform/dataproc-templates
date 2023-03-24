# Azure Blob To BigQuery

Template for reading files from Azure Blob Storage and writing them to a BigQuery table. It supports reading JSON, CSV, Parquet, Avro and Delta.IO formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `azure.storage.account`: Azure Storage Account  
* `azure.container.name`:  Azure Container Name
* `azure.sas`: Azure SAS Token
* `azure.blob.bigquery.input.location`: Azure blob of the input files (format: `"wasbs://{azure.container.name}@{azure.storage.account}.blob.core.windows.net/{input_location}"`)
* `azure.blob.bigquery.output.dataset`: BigQuery dataset for the output table
* `azure.blob.bigquery.output.table`: BigQuery output table name
* `azure.blob.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json)
* `azure.blob.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `azure.blob.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)

## Usage

```
$ python main.py --template AZUREBLOBTOBQ --help

usage: main.py --template AZUREBLOBTOBQ [-h] \
    --azure.blob.bigquery.input.location AZURE.BLOB.BIGQUERY.INPUT.LOCATION \
    --azure.blob.bigquery.output.dataset AZURE.BLOB.BIGQUERY.OUTPUT.DATASET \
    --azure.blob.bigquery.output.tableAZURE.BLOB.BIGQUERY.OUTPUT.TABLE \
    --azure.blob.bigquery.input.format {avro,parquet,csv,json} \
    --azure.blob.bigquery.temp.bucket.name AZURE.BLOB.BIGQUERY.TEMP.BUCKET.NAME \
    --azure.blob.bigquery.output.mode {overwrite,append,ignore,errorifexists} \ 
    --azure.blob.storage.account AZURE.BLOB.STORAGE.ACCOUNT \
    --azure.blob.container.name AZURE.BLOB.CONTAINER.NAME \
    --azure.blob.sas.token AZURE.BLOB.SAS.TOKEN  

optional arguments:
  -h, --help            show this help message and exit
  --azure.blob.bigquery.input.location AZURE.BLOB.BIGQUERY.INPUT.LOCATION
                        AZURE blob location of the input files
  --azure.blob.bigquery.output.dataset AZURE.BLOB.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --azure.blob.bigquery.output.table AZURE.BLOB.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --azure.blob.bigquery.input.format {avro,parquet,csv,json}
                        Input file format (one of: avro,parquet,csv,json)
  --azure.blob.bigquery.temp.bucket.name AZURE.BLOB.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --azure.blob.bigquery.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --azure.blob.storage.account AZURE.BLOB.STORAGE.ACCOUNT 
                        Azure storage account
  --azure.blob.container.name AZURE.BLOB.CONTAINER.NAME 
                        Azure container name
  --azure.blob.sas AZURE.BLOB.SAS.TOKEN 
                        Azure SAS token      
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example)  and [DeltaIO dependencies](https://docs.delta.io/latest/releases.html) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar, gs://{jars_bucket}/delta-core_2.12-1.1.0.jar" 

./bin/start.sh \
-- --template=AZUREBLOBTOBQ \
    --azure.blob.bigquery.input.format="<json|csv|parquet|avro>" \
    --azure.blob.bigquery.input.location="wasbs://{azure.container.name}@{azure.storage.account}.blob.core.windows.net/{input_location}" \
    --azure.blob.bigquery.output.dataset="<dataset>" \
    --azure.blob.bigquery.output.table="<table>" \
    --azure.blob.bigquery.output.mode=<append|overwrite|ignore|errorifexists>\
    --azure.blob.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
    --azure.blob.storage.account AZURE.BLOB.STORAGE.ACCOUNT \
    --azure.blob.container.name AZURE.BLOB.CONTAINER.NAME \
    --azure.blob.sas.token AZURE.BLOB.SAS.TOKEN 
```
