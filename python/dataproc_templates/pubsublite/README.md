## Pub/Sub Lite to Cloud Storage

Template for reading files from Pub/Sub Lite and writing them to Google Cloud Storage. It supports writing JSON, CSV, Parquet and Avro formats.


## Arguments

* `pubsublite.to.gcs.input.subscription.url`: Pub/Sub Lite Input Subscription Url
* `pubsublite.to.gcs.write.mode`: Output write mode (one of: append, update, complete)(Defaults to append)
* `pubsublite.to.gcs.output.location`: Cloud Storage Location to put Output Files (format: `gs://BUCKET/...`)
* `pubsublite.to.gcs.checkpoint.location`: Cloud Storage Checkpoint Folder Location
* `pubsublite.to.gcs.output.format`: Cloud Storage Output File Format (one of: avro,parquet,csv,json)(Defaults to csv)
* `pubsublite.to.gcs.timeout`: Time for which the subscription will be read (measured in seconds)
* `pubsublite.to.gcs.processing.time`: Time at which the query will be triggered to process input data (measured in seconds) (format: `"1 second"`)

## CSV Optional Arguments

* `pubsublite.to.gcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `pubsublite.to.gcs.output.compression`: None
* `pubsublite.to.gcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
* `pubsublite.to.gcs.output.emptyvalue`: Sets the string representation of an empty value
* `pubsublite.to.gcs.output.encoding`: Decodes the CSV files by the given encoding type
* `pubsublite.to.gcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `pubsublite.to.gcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
* `pubsublite.to.gcs.output.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `pubsublite.to.gcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `pubsublite.to.gcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `pubsublite.to.gcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `pubsublite.to.gcs.output.nullvalue`: Sets the string representation of a null value
* `pubsublite.to.gcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `pubsublite.to.gcs.output.quoteall`: A flag indicating whether all values should always be enclosed in quotes.
* `pubsublite.to.gcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `pubsublite.to.gcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `pubsublite.to.gcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format

## Usage

```
$ python main.py --template PUBSUBLITETOGCS --help

usage: main.py [-h] --pubsublite.to.gcs.input.subscription.url PUBSUBLITE.TO.GCS.INPUT.SUBSCRIPTION.URL [--pubsublite.to.gcs.write.mode {append,update,complete}]
               --pubsublite.to.gcs.output.location PUBSUBLITE.TO.GCS.OUTPUT.LOCATION 
               --pubsublite.to.gcs.checkpoint.location PUBSUBLITE.TO.GCS.CHECKPOINT.LOCATION
               [--pubsublite.to.gcs.output.format {avro,csv,json,parquet}] 
               --pubsublite.to.gcs.timeout PUBSUBLITE.TO.GCS.TIMEOUT 
               --pubsublite.to.gcs.processing.time PUBSUBLITE.TO.GCS.PROCESSING.TIME 
               [--pubsublite.to.gcs.output.ignoretrailingwhitespace PUBSUBLITE.TO.GCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--pubsublite.to.gcs.output.escape PUBSUBLITE.TO.GCS.OUTPUT.ESCAPE] 
               [--pubsublite.to.gcs.output.linesep PUBSUBLITE.TO.GCS.OUTPUT.LINESEP]
               [--pubsublite.to.gcs.output.timestampntzformat PUBSUBLITE.TO.GCS.OUTPUT.TIMESTAMPNTZFORMAT]
               [--pubsublite.to.gcs.output.quoteall PUBSUBLITE.TO.GCS.OUTPUT.QUOTEALL] 
               [--pubsublite.to.gcs.output.encoding PUBSUBLITE.TO.GCS.OUTPUT.ENCODING]
               [--pubsublite.to.gcs.output.chartoescapequoteescaping PUBSUBLITE.TO.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--pubsublite.to.gcs.output.emptyvalue PUBSUBLITE.TO.GCS.OUTPUT.EMPTYVALUE]
               [--pubsublite.to.gcs.output.timestampformat PUBSUBLITE.TO.GCS.OUTPUT.TIMESTAMPFORMAT]
               [--pubsublite.to.gcs.output.ignoreleadingwhitespace PUBSUBLITE.TO.GCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--pubsublite.to.gcs.output.sep PUBSUBLITE.TO.GCS.OUTPUT.SEP] 
               [--pubsublite.to.gcs.output.quote PUBSUBLITE.TO.GCS.OUTPUT.QUOTE]
               [--pubsublite.to.gcs.output.dateformat PUBSUBLITE.TO.GCS.OUTPUT.DATEFORMAT] 
               [--pubsublite.to.gcs.output.escapequotes PUBSUBLITE.TO.GCS.OUTPUT.ESCAPEQUOTES]
               [--pubsublite.to.gcs.output.nullvalue PUBSUBLITE.TO.GCS.OUTPUT.NULLVALUE] 
               [--pubsublite.to.gcs.output.compression PUBSUBLITE.TO.GCS.OUTPUT.COMPRESSION]
               [--pubsublite.to.gcs.output.header PUBSUBLITE.TO.GCS.OUTPUT.HEADER]

optional arguments:
  -h, --help            show this help message and exit
  --pubsublite.to.gcs.input.subscription.url PUBSUBLITE.TO.GCS.INPUT.SUBSCRIPTION.URL
                        Pub/Sub Lite Input subscription url
  --pubsublite.to.gcs.write.mode {append,update,complete}
                        Output write mode (one of: append, update, complete) (Defaults to append)
  --pubsublite.to.gcs.output.location PUBSUBLITE.TO.GCS.OUTPUT.LOCATION
                        Cloud Storage output Bucket URL
  --pubsublite.to.gcs.checkpoint.location PUBSUBLITE.TO.GCS.CHECKPOINT.LOCATION
                        Temporary folder for checkpoint location
  --pubsublite.to.gcs.output.format {avro,csv,json,parquet}
                        Output Format to Cloud Storage (one of: json, csv, avro, parquet) (Defaults to csv)
  --pubsublite.to.gcs.timeout PUBSUBLITE.TO.GCS.TIMEOUT
                        Time for which subscriptions will be read
  --pubsublite.to.gcs.processing.time PUBSUBLITE.TO.GCS.PROCESSING.TIME
                        Time interval at which the query will be triggered to process input data
  --pubsublite.to.gcs.output.ignoretrailingwhitespace PUBSUBLITE.TO.GCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --pubsublite.to.gcs.output.escape PUBSUBLITE.TO.GCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --pubsublite.to.gcs.output.linesep PUBSUBLITE.TO.GCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --pubsublite.to.gcs.output.timestampntzformat PUBSUBLITE.TO.GCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
  --pubsublite.to.gcs.output.quoteall PUBSUBLITE.TO.GCS.OUTPUT.QUOTEALL
                        A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character
  --pubsublite.to.gcs.output.encoding PUBSUBLITE.TO.GCS.OUTPUT.ENCODING
                        Specifies encoding (charset) of saved CSV files
  --pubsublite.to.gcs.output.chartoescapequoteescaping PUBSUBLITE.TO.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters
                        are different, \0 otherwise
  --pubsublite.to.gcs.output.emptyvalue PUBSUBLITE.TO.GCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --pubsublite.to.gcs.output.timestampformat PUBSUBLITE.TO.GCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --pubsublite.to.gcs.output.ignoreleadingwhitespace PUBSUBLITE.TO.GCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --pubsublite.to.gcs.output.sep PUBSUBLITE.TO.GCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --pubsublite.to.gcs.output.quote PUBSUBLITE.TO.GCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For writing, if an empty string is set, it uses
                        u0000 (null character)
  --pubsublite.to.gcs.output.dateformat PUBSUBLITE.TO.GCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --pubsublite.to.gcs.output.escapequotes PUBSUBLITE.TO.GCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --pubsublite.to.gcs.output.nullvalue PUBSUBLITE.TO.GCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --pubsublite.to.gcs.output.compression PUBSUBLITE.TO.GCS.OUTPUT.COMPRESSION
                        Compression codec to use when saving to file. This can be one of the known case-insensitive short names (none, bzip2, gzip, lz4, snappy and deflate)
  --pubsublite.to.gcs.output.header PUBSUBLITE.TO.GCS.OUTPUT.HEADER
                        Writes the names of columns as the first line. Defaults to True
```

## Required JAR files

It uses the [Pub/Sub Lite Spark SQL Streaming](https://central.sonatype.com/artifact/com.google.cloud/pubsublite-spark-sql-streaming/1.0.0) for reading data from Pub/Sub lite to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=my-project
export JARS="gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar"
export GCS_STAGING_LOCATION="gs://my-bucket"
export REGION=us-central1
	
./bin/start.sh \
-- --template=PUBSUBLITETOGCS \
    --pubsublite.to.gcs.input.subscription.url=projects/my-project/locations/us-central1/subscriptions/pubsublite-subscription,
    --pubsublite.to.gcs.write.mode=append,
    --pubsublite.to.gcs.output.location=gs://outputLocation,
    --pubsublite.to.gcs.checkpoint.location=gs://checkpointLocation,
    --pubsublite.to.gcs.output.format="csv"
    --pubsublite.to.gcs.timeout=120
    --pubsublite.to.gcs.processing.time="1 second"
```
