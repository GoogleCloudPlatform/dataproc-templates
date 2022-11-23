## Cassandra to Bigquery

Template for exporting a Cassandra table to Bigquery


## Arguments
* `cassandratobq.input.keyspace`: Input keyspace name for cassandra
* `cassandratobq.input.table`: Input table name of cassandra 
* `cassandratobq.input.host`: Cassandra Host IP 
* `cassandratobq.bigquery.location`: Dataset and Table name 
* `cassandratobq.output.mode`: Output mode of Cassandra to Bq
* `cassandratobq.temp.gcs.location`: Temp GCS location for staging


## Usage

```
$ python main.py --template CASSANDRATOBQ --help

usage: main.py --template CASSANDRATOBQ [-h] \
	--bigquery.gcs.input.table BIGQUERY.GCS.INPUT.TABLE \
	--bigquery.gcs.output.location BIGQUERY.GCS.OUTPUT.LOCATION \
	--bigquery.gcs.output.format {avro,parquet,csv,json} \
    [--bigquery.gcs.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --bigquery.gcs.input.table BIGQUERY.GCS.INPUT.TABLE
                        BigQuery Input table name
  --bigquery.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --bigquery.gcs.output.location BIGQUERY.GCS.OUTPUT.LOCATION
                        GCS location for output files
  --bigquery.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
```

## Required JAR files

This template requires the [Spark Cassandra connector](https://github.com/datastax/spark-cassandra-connector) to be available in the Dataproc cluster.
This can be downloaded using the following [link](https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.2.0/spark-cassandra-connector_2.12-3.2.0.jar)
## Example submission
Use the following command to download the jar-:
```
wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.2.0/spark-cassandra-connector_2.12-3.2.0.jar
```
A jar file named `spark-cassandra-connector_2.12-3.2.0.jar` would be downloaded, this can be passed to provide spark drivers and workers with cassandra connector classes.
```
export GCP_PROJECT=<project_id>
export JARS=spark-cassandra-connector_2.12-3.2.0.jar # Pass the downloaded jar path
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export REGION=<region>

./bin/start.sh \
-- --template=CASSANDRATOBQ \
	--bigquery.gcs.input.table=<projectId:datasetName.tableName> \
	--bigquery.gcs.output.format=<csv|parquet|avro|json> \
	--bigquery.gcs.output.mode=<overwrite|append|ignore|errorifexists> \
	--bigquery.gcs.output.location=<gs://bucket/path>
```
