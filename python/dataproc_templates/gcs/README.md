## GCS To BigQuery

Template for reading files from Google Cloud Storage and writing them to a BigQuery table. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

# Arguments

* `gcs.bigquery.input.location`: GCS location of the input files (format: `gs://BUCKET/...`)
* `gcs.bigquery.output.dataset`: BigQuery dataset for the output table
* `gcs.bigquery.output.table`: BigQuery output table name
* `gcs.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json)
* `gcs.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `gcs.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)

# Usage

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

# Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

# Example submission

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
    --gcs.bigquery.output.mode=<append|overwrite|ignore|errorifexists> \
    --gcs.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```

## GCS To JDBC

Template for reading files from Google Cloud Storage and writing them to a JDBC table. It supports reading JSON, CSV, Parquet and Avro formats.

# Arguments

* `gcs.jdbc.input.format`: Input file format (one of: avro,parquet,csv,json)
* `gcs.jdbc.input.location`: GCS location of the input files (format: `gs://BUCKET/...`)
* `gcs.jdbc.output.table`: BigQuery output table name
* `gcs.jdbc.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `gcs.jdbc.output.driver`: JDBC output driver name
* `gcs.jdbc.batch.size`: JDBC output batch size
* `gcs.jdbc.output.url`: JDBC output URL

# Usage

```
$ python main.py --template GCSTOJDBC --help

usage: main.py --template GCSTOJDBC [-h] \
    --gcs.jdbc.input.location GCS.JDBC.INPUT.LOCATION \
    --gcs.jdbc.input.format {avro,parquet,csv,json} \
    --gcs.jdbc.output.table GCS.JDBC.OUTPUT.TABLE \
    --gcs.jdbc.output.mode {overwrite,append,ignore,errorifexists} \
    --gcs.jdbc.output.driver GCS.JDBC.OUTPUT.DRIVER \
    --gcs.jdbc.batch.size GCS.JDBC.BATCH.SIZE \
    --gcs.jdbc.output.url GCS.JDBC.OUTPUT.URL


optional arguments:
  -h, --help            show this help message and exit
  --gcs.jdbc.input.location GCS.JDBC.INPUT.LOCATION
                        GCS location of the input files
  --gcs.jdbc.input.format {avro,parquet,csv,json}
                        Input file format (one of: avro,parquet,csv,json)                        
  --gcs.jdbc.output.table GCS.JDBC.OUTPUT.TABLE
                        JDBC output table name
  --gcs.jdbc.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)                        
  --gcs.jdbc.output.driver GCS.JDBC.OUTPUT.DRIVER
                        JDBC Output Driver Name
  --gcs.jdbc.batch.size GCS.JDBC.BATCH.SIZE
                        Batch size of the data means number of records wanted to insert in one round trip into JDBC Table                                               
  --gcs.jdbc.output.url GCS.JDBC.OUTPUT.URL
                        JDBC Driver URL to connect with consisting of username and passwprd as well
```

# Required JAR files

This template requires the JDBC jar file to be available in the Dataproc cluster.

# Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://spark-lib/jdbc/mysql-connector-java.jar"

./bin/start.sh \
-- --template=GCSTOBIGQUERY \
    --gcs.jdbc.input.format="<json|csv|parquet|avro>" \
    --gcs.jdbc.input.location="<gs://bucket/path>" \
    --gcs.bigquery.output.table="<table>" \
    --gcs.bigquery.output.mode=<append|overwrite|ignore|errorifexists> \
    --gcs.jdbc.output.driver="com.mysql.cj.jdbc.driver" \
    --gcs.jdbc.batch.size=1000 \
    --gcs.jdbc.output.url="jdbc:mysql://12.345.678.9:3306/test?user=root&password=root"
```

