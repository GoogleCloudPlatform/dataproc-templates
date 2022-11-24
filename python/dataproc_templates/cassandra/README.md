## Cassandra to Bigquery

Template for exporting a Cassandra table to Bigquery


## Arguments
* `cassandratobq.input.keyspace`: Input keyspace name for cassandra
* `cassandratobq.input.table`: Input table name of cassandra 
* `cassandratobq.input.host`: Cassandra Host IP 
* `cassandratobq.bigquery.location`: Dataset and Table name
* `cassandratobq.temp.gcs.location`: Temp GCS location for staging
#### Optional Arguments
* `cassandratobq.input.query` : Customised query for selective migration
* `cassandratobq.input.catalog.name`: Connection name, defaults to casscon
* `cassandratobq.output.mode`: Output mode of Cassandra to Bq, defaults to Append mode


## Usage

```
$ python main.py --template CASSANDRATOBQ --help

usage: main.py --template CASSANDRATOBQ [-h] \
	--cassandratobq.input.keyspace CASSANDRA.INPUT.KEYSPACE.NAME \
	--cassandratobq.input.table CASSANDRA.INPUT.KEYSPACE.NAME \
	--cassandratobq.input.host CASSANDRA.DATABASE.HOST.IP \
	--cassandratobq.bigquery.location BIGQUERY.DATASET.TABLE.LOCATION \
	--cassandratobq.temp.gcs.location TEMPORARY.GCS.STAGING.LOCATION \
    [ --cassandratobq.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --cassandratobq.input.query   Input query for customised migration
                       
  --cassandratobq.input.catalog.name   Cassandra connection name
                        
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
export JARS=/path/to/jar/spark-cassandra-connector_2.12-3.2.0.jar # Pass the downloaded jar path
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export REGION=<region>

./bin/start.sh \
-- --template=CASSANDRATOBQ \
	--cassandratobq.input.keyspace=<keyspace name> \
	--cassandratobq.input.table=<cassandra table name> \
	--cassandratobq.input.host=<cassandra host IP> \
	--cassandratobq.bigquery.location=<dataset.tablename> \
	--cassandratobq.temp.gcs.location=<staging bucket name> \
	--cassandratobq.output.mode=<overwrite|append|ignore|errorifexists>
```
