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
This can be downloaded using the following [link](https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.2.0/spark-cassandra-connector-assembly_2.12-3.2.0.jar).
Another jar of POSIX handler is also required which can be downloaded from this [link](https://repo1.maven.org/maven2/com/github/jnr/jnr-posix/3.1.8/jnr-posix-3.1.8.jar)
## Example submission
Use the following command to download the jar-:
```
wget https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.2.0/spark-cassandra-connector-assembly_2.12-3.2.0.jar
```
```
wget https://repo1.maven.org/maven2/com/github/jnr/jnr-posix/3.1.8/jnr-posix-3.1.8.jar
```
Jar files named `spark-cassandra-connector-assembly_2.12-3.2.0.jar` and `jnr-posix-3.1.8.jar` would be downloaded, this can be passed to provide spark drivers and workers with cassandra connector class and its dependencies.
```
export GCP_PROJECT=<project_id>
export JARS="gs://path/to/jar/spark-cassandra-connector_2.12-3.2.0.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar,gs://path/to/jnr-posix-3.1.8.jar" # Pass the downloaded jar path and bigquery connector jar
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
## Sample Submission
After downloading the jar, a sample submission -:
```
export GCP_PROJECT=my-project
export REGION=us-central1
export GCS_STAGING_LOCATION=gs://staging_bucket
export SUBNET=projects/my-project/regions/us-central1/subnetworks/default
export JARS="gs://jar-bucket/spark-cassandra-connector-assembly_2.12-3.2.0.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar,gs://jar-bucket/jnr-posix-3.1.8.jar"


    ./bin/start.sh \
-- --template=CASSANDRATOBQ \
--cassandratobq.input.keyspace=tk1 \
--cassandratobq.input.table=emp \
--cassandratobq.input.host=10.1X8.X.XX \
--cassandratobq.bigquery.location=demo.cassandramig \
--cassandratobq.temp.gcs.location=bucketname \
--cassandratobq.input.query="select emp_id from casscon.tk1.emp"
```
