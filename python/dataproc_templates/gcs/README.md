## GCS To BigQuery

Template for reading files from Google Cloud Storage and writing them to a BigQuery table. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `gcs.bigquery.input.location`: GCS location of the input files (format: `gs://bucket/...`)
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

It uses the Apache HBase Spark Connector to write to BigTable.

This [tutorial](https://cloud.google.com/dataproc/docs/tutorials/spark-hbase#dataproc_hbase_tutorial_view_code-python) shows how to run a Spark/PySpark job connecting to BigTable.  
However, it focuses in running the job using a Dataproc cluster, and not Dataproc Serverless.  
Here in this template, you will notice that there are different configuration steps for the PySpark job to successfully run using Dataproc Serverless, connecting to BigTable using the HBase interface.

You can also check out the [differences between HBase and Cloud Bigtable](https://cloud.google.com/bigtable/docs/hbase-differences).

## Requirements

1) Configure the [hbase-site.xml](./hbase-site.xml) ([reference](https://cloud.google.com/bigtable/docs/hbase-connecting#creating_the_hbase-sitexml_file)) with your BigTable instance reference
    - The hbase-site.xml needs to be available in some path of the container image used by Dataproc Serverless.  
    - For that, you need to build and host a [customer container image](https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers#submit_a_spark_batch_workload_using_a_custom_container_image) in GCP Container Registry.  
       - You can use and adapt the Dockerfile from the guide above, building and pushing it to GCP Container Registry with:
         - _docker build -t "${IMAGE}" ._
         - _docker push "${IMAGE}"_
    - But first, add the following layer to the Dockerfile, for it to copy your local hbase-site.xml to the container image:
      - _COPY hbase-site.xml /etc/hbase/conf/_ 
    - An SPARK_EXTRA_CLASSPATH environment variable should also be set to the same path when submitting the job.
    ```
    (./bin/start.sh ...)
    --container-image="gcr.io/<your_project>/<your_custom_image>:<your_version>"  # image with hbase-site.xml in /etc/hbase/conf/
    --properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/'
    ```

2) Configure the [hbase-catalog.json](./hbase-catalog.json) with your HBase catalog (table reference and schema)
    - The hbase-catalog.json should be passed using the --files, and it is read by the template Spark application:
    ```
    (./bin/start.sh ...)
    --files="./dataproc_templates/gcs/hbase-catalog.json" 
    ```

3) Use the [cbt tool](https://cloud.google.com/bigtable/docs/managing-tables) to manage your Bigtable table schema, column families, etc, to match the provided HBase catalog.

## Required JAR files

Some HBase and BigTable dependencies are required to be passed when submitting the job.  
These dependencies need to be passed by using the --jars flag, or, in the case of Dataproc Templates, using the JARS environment variable.  
Some dependencies (jars) must be downloaded from [MVN Repository](https://mvnrepository.com/) and stored your GCS bucket (create one to store the dependencies).  

- **[Apache HBase Spark Connector](https://mvnrepository.com/artifact/org.apache.hbase.connectors.spark/hbase-spark) dependencies (already mounted in Dataproc Serverless, so you refer to them using file://):**
   - file:///usr/lib/spark/external/hbase-spark-protocol-shaded.jar
   - file:///usr/lib/spark/external/hbase-spark.jar
   
- **Bigtable dependency:**
  - gs://<your_bucket_to_store_dependencies>/bigtable-hbase-2.x-hadoop-2.3.0.jar
    - Download it from [here](https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase/2.3.0/bigtable-hbase-2.3.0.jar)

- **HBase dependencies:**
  - gs://<your_bucket_to_store_dependencies>/hbase-common-2.4.12.jar
      - Download it from [here](https://repo1.maven.org/maven2/org/apache/hbase/hbase-common/2.4.12/hbase-common-2.4.12.jar)
  - gs://<your_bucket_to_store_dependencies>/hbase-client-2.4.12.jar
      - Download it from [here](https://repo1.maven.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar)
  - gs://<your_bucket_to_store_dependencies>/hbase-server-2.4.12.jar
      - Download it from [here](https://repo1.maven.org/maven2/org/apache/hbase/hbase-server/2.4.12/hbase-server-2.4.12.jar)
  - gs://<your_bucket_to_store_dependencies>/hbase-mapreduce-2.4.12.jar
      - Download it from [here](https://repo1.maven.org/maven2/org/apache/hbase/hbase-mapreduce/2.4.12/hbase-mapreduce-2.4.12.jar)
  - gs://<your_bucket_to_store_dependencies>/hbase-shaded-miscellaneous-4.1.0.jar
      - Download it from [here](https://repo1.maven.org/maven2/org/apache/hbase/thirdparty/hbase-shaded-miscellaneous/4.1.0/hbase-shaded-miscellaneous-4.1.0.jar)
  - gs://<your_bucket_to_store_dependencies>/hbase-shaded-protobuf-4.1.0.jar
      - Download it from [here](https://repo1.maven.org/maven2/org/apache/hbase/thirdparty/hbase-shaded-protobuf/4.1.0/hbase-shaded-protobuf-4.1.0.jar)

## Arguments

* `gcs.bigquery.input.location`: GCS location of the input files (format: `gs://<bucket>/...`)
* `gcs.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json)

Note: the Bigtable table output configuration is done from the hbase-catalog.json file

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

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://<your_bucket_to_store_dependencies>/hbase-shaded-protobuf-4.1.0.jar, \
             gs://<your_bucket_to_store_dependencies>/bigtable-hbase-2.x-hadoop-2.3.0.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-common-2.4.12.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-client-2.4.12.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-server-2.4.12.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-mapreduce-2.4.12.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-shaded-miscellaneous-4.1.0.jar, \
             file:///usr/lib/spark/external/hbase-spark-protocol-shaded.jar, \
             file:///usr/lib/spark/external/hbase-spark.jar"

./bin/start.sh \
--container-image="gcr.io/<your_project>/<your_custom_image>:<your_version>" \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/' \ # image with hbase-site.xml in /etc/hbase/conf/
--files="./dataproc_templates/gcs/hbase-catalog.json" \
-- --template=GCSTOBIGTABLE \
   --gcs.bigtable.input.format="<json|csv|parquet|avro>" \
   --gcs.bigtable.input.location="<gs://bucket/path>"
```