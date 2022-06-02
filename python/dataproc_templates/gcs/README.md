## GCS To BigQuery

Template for reading files from Google Cloud Storage and writing them to a BigQuery table. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `gcs.bigquery.input.location`: GCS location of the input files (format: `gs://BUCKET/...`)
* `gcs.bigquery.output.dataset`: BigQuery dataset for the output table
* `gcs.bigquery.output.table`: BigQuery output table name
* `gcs.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json)
* `gcs.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `gcs.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)

## Usage

```
$ python main.py --template GCSTOBIGQUERY --help

usage: main.py --template GCSTOBIGQUERY [-h] \
    --gcs.bigquery.input.location GCS.BIGQUERY.INPUT.LOCATION \
    --gcs.bigquery.output.dataset GCS.BIGQUERY.OUTPUT.DATASET \
    --gcs.bigquery.output.table GCS.BIGQUERY.OUTPUT.TABLE \
    --gcs.bigquery.input.format {avro,parquet,csv,json} \
    --gcs.bigquery.temp.bucket.name GCS.BIGQUERY.TEMP.BUCKET.NAME \
    [--gcs.bigquery.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --gcs.bigquery.input.location GCS.BIGQUERY.INPUT.LOCATION
                        GCS location of the input files
  --gcs.bigquery.output.dataset GCS.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --gcs.bigquery.output.table GCS.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --gcs.bigquery.input.format {avro,parquet,csv,json}
                        Input file format (one of: avro,parquet,csv,json)
  --gcs.bigquery.temp.bucket.name GCS.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --gcs.bigquery.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

./bin/start.sh \
-- --template=GCSTOBIGQUERY \
    --gcs.bigquery.input.format="<json|csv|parquet|avro>" \
    --gcs.bigquery.input.location="<gs://bucket/path>" \
    --gcs.bigquery.output.dataset="<dataset>" \
    --gcs.bigquery.output.table="<table>" \
    --gcs.bigquery.output.mode=<append|overwrite|ignore|errorifexists>\
    --gcs.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```


## GCS To BigTable

Template for reading files from Google Cloud Storage and writing them to a BigTable table. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the [Apache HBase Spark Connector](https://mvnrepository.com/artifact/org.apache.hbase.connectors.spark/hbase-spark) to write to BigTable.

## Requirements

- Configure the hbase-catalog.json with your catalog (table reference and schema)
- Configure the hbase-site.xml with your BigTable instance reference

The hbase-site.xml needs to be available in the container image used by Dataproc Serverless.  
You need to host a customer container image in GCP Container Registry.  
A environment variable should be set to its path as well when submitting the job.  

```
--container-image="gcr.io/<your_project>/<your_custom_image>:<your_version>"  # image with hbase-site.xml in /etc/hbase/conf/
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/'
```

The hbase-catalog.json should be passed using the --files, and it is read by the Spark application:
```
--files="./dataproc_templates/gcs/hbase-catalog.json" 
```

## Arguments

* `gcs.bigquery.input.location`: GCS location of the input files (format: `gs://<bucket>/...`)
* `gcs.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json)

## Usage

```
$ python main.py --template GCSTOBIGTABLE --help

usage: main.py [-h] --gcs.bigtable.input.location GCS.BIGTABLE.INPUT.LOCATION
                    --gcs.bigtable.input.format {avro,parquet,csv,json}

optional arguments:
  -h, --help            show this help message and exit
  --gcs.bigtable.input.location GCS.BIGTABLE.INPUT.LOCATION
                        GCS location of the input files
  --gcs.bigtable.input.format {avro,parquet,csv,json}
                        Input file format (one of: avro,parquet,csv,json)
```

## Required JAR files

Some HBase and BigTable dependencies are required to be passed:

- gs://deps-dataproc-template/hbase-spark-1.0.1-spark_3.2-scala_2.12.jar
- gs://deps-dataproc-template/hbase-spark-protocol-shaded-1.0.1-spark_3.2-scala_2.12.jar
- gs://deps-dataproc-template/bigtable-hbase-2.x-hadoop-2.3.0.jar
- gs://deps-dataproc-template/hbase-common-2.4.12.jar,
- gs://deps-dataproc-template/hbase-client-2.4.12.jar
- gs://deps-dataproc-template/hbase-server-2.4.12.jar
- gs://deps-dataproc-template/hbase-mapreduce-2.4.12.jar
- gs://deps-dataproc-template/hbase-shaded-miscellaneous-4.1.0.jar
- gs://deps-dataproc-template/hbase-shaded-protobuf-4.1.0.jar

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://deps-dataproc-template/hbase-shaded-protobuf-4.1.0.jar,gs://deps-dataproc-template/bigtable-hbase-2.x-hadoop-2.3.0.jar,gs://deps-dataproc-template/hbase-common-2.4.12.jar,gs://deps-dataproc-template/hbase-client-2.4.12.jar,gs://deps-dataproc-template/hbase-server-2.4.12.jar,gs://deps-dataproc-template/hbase-spark-1.0.1-spark_3.2-scala_2.12.jar,gs://deps-dataproc-template/hbase-spark-protocol-shaded-1.0.1-spark_3.2-scala_2.12.jar,gs://deps-dataproc-template/hbase-mapreduce-2.4.12.jar,gs://deps-dataproc-template/hbase-shaded-miscellaneous-4.1.0.jar"

./bin/start.sh \
--container-image="gcr.io/<your_project>/<your_custom_image>:<your_version>" \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/' \
--files="./dataproc_templates/gcs/hbase-catalog.json" \
-- --template=GCSTOBIGTABLE \
   --gcs.bigtable.input.format="<json|csv|parquet|avro>" \
   --gcs.bigtable.input.location="<gs://bucket/path>"
```