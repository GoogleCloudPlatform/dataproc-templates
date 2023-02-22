## Cassandra to Bigquery

Template for exporting a Cassandra table to Bigquery


## Arguments
* `cassandratobq.input.host`: Cassandra Host IP 
* `cassandratobq.bigquery.location`: Dataset and Table name
* `cassandratobq.temp.gcs.location`: Temp GCS location for staging
#### Optional Arguments
* `cassandratobq.input.keyspace`: Input keyspace name for cassandra
* `cassandratobq.input.table`: Input table name of cassandra
* `cassandratobq.input.query` : Customised query for selective migration
* `cassandratobq.input.catalog.name`: Connection name, defaults to casscon
* `cassandratobq.output.mode`: Output mode of Cassandra to Bq, defaults to Append mode


## Usage

```
$ python main.py --template CASSANDRATOBQ --help

usage: main.py --template CASSANDRATOBQ [-h] \
	--cassandratobq.input.host CASSANDRA.DATABASE.HOST.IP \
	--cassandratobq.bigquery.location BIGQUERY.DATASET.TABLE.LOCATION \
	--cassandratobq.temp.gcs.location TEMPORARY.GCS.STAGING.LOCATION \

optional arguments:
  -h, --help            show this help message and exit
  --cassandratobq.input.keyspace CASSANDRA.INPUT.KEYSPACE \
  --cassandratobq.input.table CASSANDRA.INPUT.TABLE \
  --cassandratobq.output.mode {overwrite,append,ignore,errorifexists} \
  --cassandratobq.input.query CASSANDRA.INPUT.QUERY \
  --cassandratobq.input.catalog.name CASSANDRA.INPUT.CATALOG.NAME
```

**Note:** Make sure that either ```cassandratobq.input.query``` or both ```cassandratobq.input.keyspace``` and ```cassandratobq.input.table``` is provided. Setting or not setting all three properties at the same time will throw an error.

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
--cassandratobq.input.host=10.1X8.X.XX \
--cassandratobq.bigquery.location=demo.cassandramig \
--cassandratobq.temp.gcs.location=bucketname \
--cassandratobq.input.query="select emp_id from casscon.tk1.emp"
```


## Cassandra to GCS

Template for exporting a Cassandra table to GCS


## Arguments
* `cassandratogcs.input.keyspace`: Input keyspace name for cassandra (not required when query is present)
* `cassandratogcs.input.table`: Input table name of cassandra (not required when query is present)
* `cassandratogcs.input.host`: Cassandra Host IP 
* `cassandratogcs.output.format` : Output File Format
* `cassandratogcs.output.path`: GCS Bucket Path
* `cassandratogcs.output.savemode`: Output mode of Cassandra to GCS
#### Optional Arguments
* `cassandratobq.input.query` : Customised query for selective migration
* `cassandratogcs.input.catalog.name`: Connection name, defaults to casscon


## Usage

```
$ python main.py --template CASSANDRATOGCS --help

usage: main.py --template CASSANDRATOGCS [-h] \
	--cassandratogcs.input.host CASSANDRATOGCS.INPUT.HOST \
	--cassandratogcs.output.format CASSANDRATOGCS.OUTPUT.FORMAT \
	--cassandratogcs.output.path CASSANDRATOGCS.OUTPUT.PATH \
	--cassandratogcs.temp.gcs.location TEMPORARY.GCS.STAGING.LOCATION \
    --cassandratogcs.output.savemode {overwrite,append,ignore,errorifexists}

optional arguments:
  -h, --help            show this help message and exit
  --cassandratogcs.input.query   Input query for customised migration
                       
  --cassandratogcs.input.catalog.name   Cassandra connection name

  --cassandratogcs.input.keyspace CASSANDRATOGCS.INPUT.KEYSPACE \

  --cassandratogcs.input.table CASSANDRATOGCS.INPUT.TABLE \
                        
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
export REGION=<region>

./bin/start.sh \
-- --template=CASSANDRATOGCS \
	--cassandratogcs.input.keyspace=<keyspace name> \
	--cassandratogcs.input.table=<cassandra table name> \
	--cassandratogcs.input.host=<cassandra host IP> \
	--cassandratogcs.output.format=<output file format> \
	--cassandratogcs.output.path=<gcs output bucket path> \
	--cassandratogcs.output.savemode=<overwrite|append|ignore|errorifexists>
```
One can add additional property to submit query (If query is provided, don't provide keyspace and table). Please see format below-:
```
--cassandratogcs.input.catalog.name=<catalog-name>
--cassandratogcs.input.query="select * from <catalog-name>.<keyspace-name>.<table-name>"
```
Note-: ```cassandratogcs.input.catalog.name=<catalog-name>``` is used to provide a name to the connection with Cassandra. This name is used for querying purpose and has a default value of ```casscon``` if nothing is passed. 
To query using default catalog name -:
```
--templateProperty cassandratogcs.input.query="select * from casscon.<keyspace-name>.<table-name>"
```
You can replace the ```casscon``` with your catalog name if it is passed. This is an important step to query the data from Cassandra. Additional details on usage of query can be found in this [link](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/14_data_frames.md).
## Sample Submission
After downloading the jar, a sample submission -:
```
export GCP_PROJECT=my-project
export REGION=us-central1
export SUBNET=projects/my-project/regions/us-central1/subnetworks/default
export JARS="gs://jar-bucket/spark-cassandra-connector-assembly_2.12-3.2.0.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar,gs://jar-bucket/jnr-posix-3.1.8.jar"


    ./bin/start.sh \
-- --template=CASSANDRATOGCS \
--cassandratogcs.input.keyspace=tk1 \
--cassandratogcs.input.table=emp \
--cassandratogcs.input.host=10.1X8.X.XX \
--cassandratogcs.output.format=csv \
--cassandratogcs.output.path=bucketname \
--cassandratogcs.output.savemode=overwrite \
--cassandratogcs.input.query="select emp_id from casscon.tk1.emp"
```

**Note:** Make sure that either ```cassandratogcs.input.query``` or both ```cassandratogcs.input.keyspace``` and ```cassandratogcs.input.table``` is provided. Setting or not setting all three properties at the same time will throw an error.


