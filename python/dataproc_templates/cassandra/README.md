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
* `cassandratogcs.output.format`: Output File Format
* `cassandratogcs.output.path`: GCS Bucket Path
* `cassandratogcs.output.savemode`: Output mode of Cassandra to GCS
#### Optional Arguments
* `cassandratobq.input.query`: Customised query for selective migration
* `cassandratogcs.input.catalog.name`: Connection name, defaults to casscon
* `cassandratogcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `cassandratogcs.output.compression`: Compression codec to use when saving to file. This can be one of the known case-insensitive short names (none, bzip2, gzip, lz4, snappy and deflate)
* `cassandratogcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
* `cassandratogcs.output.emptyvalue`: Sets the string representation of an empty value
* `cassandratogcs.output.encoding`: Specifies encoding (charset) of saved CSV files
* `cassandratogcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `cassandratogcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
* `cassandratogcs.output.header`: Writes the names of columns as the first line. Defaults to True
* `cassandratogcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `cassandratogcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `cassandratogcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `cassandratogcs.output.nullvalue`: Sets the string representation of a null value
* `cassandratogcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For writing, if an empty string is set, it uses u0000 (null character)
* `cassandratogcs.output.quoteall`: A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character
* `cassandratogcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `cassandratogcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `cassandratogcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format


## Usage

```
$ python main.py --template CASSANDRATOGCS --help

usage: main.py [-h]
               --cassandratogcs.input.host CASSANDRATOGCS.INPUT.HOST
               --cassandratogcs.output.format {avro,parquet,csv,json}
               --cassandratogcs.output.path CASSANDRATOGCS.OUTPUT.PATH
               [--cassandratogcs.output.savemode {overwrite,append,ignore,errorifexists}]
               [--cassandratogcs.input.catalog.name CASSANDRATOGCS.INPUT.CATALOG.NAME]
               [--cassandratogcs.input.query CASSANDRATOGCS.INPUT.QUERY]
               [--cassandratogcs.input.keyspace CASSANDRATOGCS.INPUT.KEYSPACE]
               [--cassandratogcs.input.table CASSANDRATOGCS.INPUT.TABLE]
               [--cassandratogcs.output.chartoescapequoteescaping CASSANDRATOGCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--cassandratogcs.output.compression CASSANDRATOGCS.OUTPUT.COMPRESSION]
               [--cassandratogcs.output.dateformat CASSANDRATOGCS.OUTPUT.DATEFORMAT]
               [--cassandratogcs.output.emptyvalue CASSANDRATOGCS.OUTPUT.EMPTYVALUE]
               [--cassandratogcs.output.encoding CASSANDRATOGCS.OUTPUT.ENCODING]
               [--cassandratogcs.output.escape CASSANDRATOGCS.OUTPUT.ESCAPE]
               [--cassandratogcs.output.escapequotes CASSANDRATOGCS.OUTPUT.ESCAPEQUOTES]
               [--cassandratogcs.output.header CASSANDRATOGCS.OUTPUT.HEADER]
               [--cassandratogcs.output.ignoreleadingwhitespace CASSANDRATOGCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--cassandratogcs.output.ignoretrailingwhitespace CASSANDRATOGCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--cassandratogcs.output.linesep CASSANDRATOGCS.OUTPUT.LINESEP]
               [--cassandratogcs.output.nullvalue CASSANDRATOGCS.OUTPUT.NULLVALUE]
               [--cassandratogcs.output.quote CASSANDRATOGCS.OUTPUT.QUOTE]
               [--cassandratogcs.output.quoteall CASSANDRATOGCS.OUTPUT.QUOTEALL]
               [--cassandratogcs.output.sep CASSANDRATOGCS.OUTPUT.SEP]
               [--cassandratogcs.output.timestampformat CASSANDRATOGCS.OUTPUT.TIMESTAMPFORMAT]
               [--cassandratogcs.output.timestampntzformat CASSANDRATOGCS.OUTPUT.TIMESTAMPNTZFORMAT]

options:
  -h, --help            show this help message and exit
  --cassandratogcs.input.host CASSANDRATOGCS.INPUT.HOST
                        CASSANDRA Cloud Storage Input Host IP
  --cassandratogcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --cassandratogcs.output.path CASSANDRATOGCS.OUTPUT.PATH
                        Cloud Storage location for output files
  --cassandratogcs.output.savemode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --cassandratogcs.input.catalog.name CASSANDRATOGCS.INPUT.CATALOG.NAME
                        To provide a name for connection between Cassandra and GCS
  --cassandratogcs.input.query CASSANDRATOGCS.INPUT.QUERY
                        Optional query for selective exports
  --cassandratogcs.input.keyspace CASSANDRATOGCS.INPUT.KEYSPACE
                        CASSANDRA Cloud Storage Input Keyspace
  --cassandratogcs.input.table CASSANDRATOGCS.INPUT.TABLE
                        CASSANDRA Cloud Storage Input Table
  --cassandratogcs.output.chartoescapequoteescaping CASSANDRATOGCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --cassandratogcs.output.compression CASSANDRATOGCS.OUTPUT.COMPRESSION
  --cassandratogcs.output.dateformat CASSANDRATOGCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --cassandratogcs.output.emptyvalue CASSANDRATOGCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --cassandratogcs.output.encoding CASSANDRATOGCS.OUTPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --cassandratogcs.output.escape CASSANDRATOGCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --cassandratogcs.output.escapequotes CASSANDRATOGCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --cassandratogcs.output.header CASSANDRATOGCS.OUTPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --cassandratogcs.output.ignoreleadingwhitespace CASSANDRATOGCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --cassandratogcs.output.ignoretrailingwhitespace CASSANDRATOGCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --cassandratogcs.output.linesep CASSANDRATOGCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --cassandratogcs.output.nullvalue CASSANDRATOGCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --cassandratogcs.output.quote CASSANDRATOGCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --cassandratogcs.output.quoteall CASSANDRATOGCS.OUTPUT.QUOTEALL
  --cassandratogcs.output.sep CASSANDRATOGCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --cassandratogcs.output.timestampformat CASSANDRATOGCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --cassandratogcs.output.timestampntzformat CASSANDRATOGCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
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
--cassandratogcs.output.header=false \
--cassandratogcs.output.timestampntzformat="yyyy-MM-dd HH:mm:ss"
```

**Note:** Make sure that either ```cassandratogcs.input.query``` or both ```cassandratogcs.input.keyspace``` and ```cassandratogcs.input.table``` is provided. Setting or not setting all three properties at the same time will throw an error.


