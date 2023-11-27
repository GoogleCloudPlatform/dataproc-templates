## Mongo to Cloud Storage

Template for exporting a MongoDB Collection to files in Google Cloud Storage. It supports writing JSON, CSV, Parquet and Avro formats.

It uses the [MongoDB Spark Connector](https://www.mongodb.com/products/spark-connector) and [MongoDB Java Driver](https://jar-download.com/?search_box=mongo-java-driver) for reading data from MongoDB Collections.

## Arguments

- `mongo.gcs.input.uri`: MongoDB Connection String as an Input URI (format: `mongodb://host_name:port_no`)
- `mongo.gcs.input.database`: MongoDB Database Name (format: Database_name)
- `mongo.gcs.input.collection`: MongoDB Input Collection Name (format: Collection_name)
- `mongo.gcs.output.format`: Cloud Storage Output File Format (one of: avro,parquet,csv,json)
- `mongo.gcs.output.location`: Cloud Storage Location to put Output Files (format: `gs://BUCKET/...`)
- `mongo.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)

#### Optional Arguments

- `mongo.gcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
- `mongo.gcs.output.compression`: None
- `mongo.gcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
- `mongo.gcs.output.emptyvalue`: Sets the string representation of an empty value
- `mongo.gcs.output.encoding`: Decodes the CSV files by the given encoding type
- `mongo.gcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
- `mongo.gcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
- `mongo.gcs.output.header`: Uses the first line of CSV file as names of columns. Defaults to True
- `mongo.gcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
- `mongo.gcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
- `mongo.gcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
- `mongo.gcs.output.nullvalue`: Sets the string representation of a null value
- `mongo.gcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
- `mongo.gcs.output.quoteall`: None
- `mongo.gcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
- `mongo.gcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
- `mongo.gcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format

## Usage

```
$ python main.py --template MONGOTOGCS --help

usage: main.py [-h]
               --mongo.gcs.input.uri MONGO.GCS.INPUT.URI
               --mongo.gcs.input.database MONGO.GCS.INPUT.DATABASE
               --mongo.gcs.input.collection MONGO.GCS.INPUT.COLLECTION
               --mongo.gcs.output.format {avro,parquet,csv,json}
               --mongo.gcs.output.location MONGO.GCS.OUTPUT.LOCATION
               [--mongo.gcs.output.mode {overwrite,append,ignore,errorifexists}]
               [--mongo.gcs.output.chartoescapequoteescaping MONGO.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--mongo.gcs.output.compression MONGO.GCS.OUTPUT.COMPRESSION]
               [--mongo.gcs.output.dateformat MONGO.GCS.OUTPUT.DATEFORMAT]
               [--mongo.gcs.output.emptyvalue MONGO.GCS.OUTPUT.EMPTYVALUE]
               [--mongo.gcs.output.encoding MONGO.GCS.OUTPUT.ENCODING]
               [--mongo.gcs.output.escape MONGO.GCS.OUTPUT.ESCAPE]
               [--mongo.gcs.output.escapequotes MONGO.GCS.OUTPUT.ESCAPEQUOTES]
               [--mongo.gcs.output.header MONGO.GCS.OUTPUT.HEADER]
               [--mongo.gcs.output.ignoreleadingwhitespace MONGO.GCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--mongo.gcs.output.ignoretrailingwhitespace MONGO.GCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--mongo.gcs.output.linesep MONGO.GCS.OUTPUT.LINESEP]
               [--mongo.gcs.output.nullvalue MONGO.GCS.OUTPUT.NULLVALUE]
               [--mongo.gcs.output.quote MONGO.GCS.OUTPUT.QUOTE] [--mongo.gcs.output.quoteall MONGO.GCS.OUTPUT.QUOTEALL]
               [--mongo.gcs.output.sep MONGO.GCS.OUTPUT.SEP]
               [--mongo.gcs.output.timestampformat MONGO.GCS.OUTPUT.TIMESTAMPFORMAT]
               [--mongo.gcs.output.timestampntzformat MONGO.GCS.OUTPUT.TIMESTAMPNTZFORMAT]

options:
  -h, --help            show this help message and exit
  --mongo.gcs.input.uri MONGO.GCS.INPUT.URI
                        MONGO Cloud Storage Input Connection Uri
  --mongo.gcs.input.database MONGO.GCS.INPUT.DATABASE
                        MONGO Cloud Storage Input Database Name
  --mongo.gcs.input.collection MONGO.GCS.INPUT.COLLECTION
                        MONGO Cloud Storage Input Collection Name
  --mongo.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --mongo.gcs.output.location MONGO.GCS.OUTPUT.LOCATION
                        Cloud Storage location for output files
  --mongo.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --mongo.gcs.output.chartoescapequoteescaping MONGO.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --mongo.gcs.output.compression MONGO.GCS.OUTPUT.COMPRESSION
  --mongo.gcs.output.dateformat MONGO.GCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --mongo.gcs.output.emptyvalue MONGO.GCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --mongo.gcs.output.encoding MONGO.GCS.OUTPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --mongo.gcs.output.escape MONGO.GCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --mongo.gcs.output.escapequotes MONGO.GCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --mongo.gcs.output.header MONGO.GCS.OUTPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --mongo.gcs.output.ignoreleadingwhitespace MONGO.GCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --mongo.gcs.output.ignoretrailingwhitespace MONGO.GCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --mongo.gcs.output.linesep MONGO.GCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --mongo.gcs.output.nullvalue MONGO.GCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --mongo.gcs.output.quote MONGO.GCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --mongo.gcs.output.quoteall MONGO.GCS.OUTPUT.QUOTEALL
  --mongo.gcs.output.sep MONGO.GCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --mongo.gcs.output.timestampformat MONGO.GCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --mongo.gcs.output.timestampntzformat MONGO.GCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
```

## Required JAR files

This template requires the [MongoDB Spark Connector](https://www.mongodb.com/products/spark-connector) and [MongoDB Java Driver](https://jar-download.com/?search_box=mongo-java-driver) to be available in the Dataproc cluster. The recommended versions for Mongo Java Driver is 3.9.1 and for Mongo Spark Connector is 2.12-2.4.0.

## Example submission

```
export GCP_PROJECT=my-project
export JARS="gs://spark-lib/mongodb/mongo-spark-connector_2.12-2.4.0.jar,gs://spark-lib/mongodb/mongo-java-driver-3.9.1.jar"
export GCS_STAGING_LOCATION="gs://my-bucket"
export REGION=us-central1

./bin/start.sh \
-- --template=MONGOTOGCS \
    --mongo.gcs.input.uri="mongodb://10.0.0.57:27017" \
    --mongo.gcs.input.database="demo" \
    --mongo.gcs.input.collection="analysis" \
    --mongo.gcs.output.format="avro" \
    --mongo.gcs.output.location="gs://my-output/mongogcsoutput" \
    --mongo.gcs.output.mode="overwrite"
```

# Mongo To BigQuery

Template for exporting a MongoDB collection to a BigQuery table.

## Required JAR files

It uses the [MongoDB Spark Connector](https://www.mongodb.com/products/spark-connector) and [MongoDB Java Driver](https://jar-download.com/?search_box=mongo-java-driver) for reading data from MongoDB Collections. To write to BigQuery, the template needs [Spark BigQuery Connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector).

This template has been tested with the following versions of the above mentioned JAR files:

1. MongoDB Spark Connector: 2.12-2.4.0
2. MongoDB Java Driver: 3.9.1
3. Spark BigQuery Connector: 2.12

## Arguments

- `mongo.bq.input.uri`: MongoDB Connection String as an Input URI (format: `mongodb://host_name:port_no`)
- `mongo.bq.input.database`: MongoDB Database Name (format: Database_name)
- `mongo.bq.input.collection`: MongoDB Input Collection Name (format: Collection_name)
- `mongo.bq.output.dataset`: BigQuery dataset id (format: Dataset_id)
- `mongo.bq.output.table`: BigQuery table name (format: Table_name)
- `mongo.bq.temp.bucket.name`: Cloud Storage bucket name to store temporary files (format: Bucket_name)

#### Optional Arguments

- `mongo.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)

## Usage

```
$ python main.py --template MONGOTOBIGQUERY --help
usage: main.py [-h] --mongo.bq.input.uri MONGO.BQ.INPUT.URI
               --mongo.bq.input.database MONGO.BQ.INPUT.DATABASE
               --mongo.bq.input.collection MONGO.BQ.INPUT.COLLECTION
               --mongo.bq.output.dataset MONGO.BQ.OUTPUT.DATASET
               --mongo.bq.output.table MONGO.BQ.OUTPUT.TABLE
               --mongo.bq.output.mode {overwrite,append,ignore,errorifexists}
               --mongo.bq.temp.bucket.name MONGO.BQ.TEMP.BUCKET.NAME

options:
  -h, --help            show this help message and exit
  --mongo.bq.input.uri MONGO.BQ.INPUT.URI
                        Mongo Input Connection Uri
  --mongo.bq.input.database MONGO.BQ.INPUT.DATABASE
                        Mongo Input Database Name
  --mongo.bq.input.collection MONGO.BQ.INPUT.COLLECTION
                        Mongo Input Collection Name
  --mongo.bq.output.dataset MONGO.BQ.OUTPUT.DATASET
                        BigQuery Output Dataset Name
  --mongo.bq.output.table MONGO.BQ.OUTPUT.TABLE
                        BigQuery Output Table Name
  --mongo.bq.output.mode {overwrite,append,ignore,errorifexists}
                        BigQuery Output write mode (one of:
                        append,overwrite,ignore,errorifexists) (Defaults to
                        append)
  --mongo.bq.temp.bucket.name MONGO.BQ.TEMP.BUCKET.NAME
                        GCS Temp Bucket Name

```

## Example submission

```
export GCP_PROJECT=my-project
export JARS="gs://spark-lib/mongodb/mongo-spark-connector_2.12-2.4.0.jar,gs://spark-lib/mongodb/mongo-java-driver-3.9.1.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
export GCS_STAGING_LOCATION="gs://my-bucket"
export REGION=us-central1
export SUBNET=projects/my-project/regions/us-central1/subnetworks/test-subnet

./bin/start.sh \
-- --template=MONGOTOBIGQUERY \
    --mongo.bq.input.uri="mongodb://10.0.0.57:27017" \
    --mongo.bq.input.database="demo" \
    --mongo.bq.input.collection="dummyusers" \
    --mongo.bq.output.dataset="my-project.test_dataset" \
    --mongo.bq.output.table="dummyusers" \
    --mongo.bq.output.mode="append" \
    --mongo.bq.temp.bucket.name="my-staging-bucket"
```
