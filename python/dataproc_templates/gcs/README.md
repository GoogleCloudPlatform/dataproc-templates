# Cloud Storage To BigQuery

Template for reading files from Cloud Storage and writing them to a BigQuery table. It supports reading JSON, CSV, Parquet, Avro and Delta formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments
* `gcs.bigquery.input.location`: Cloud Storage location of the input files (format: `gs://bucket/...`)
* `gcs.bigquery.output.dataset`: BigQuery dataset for the output table
* `gcs.bigquery.output.table`: BigQuery output table name
* `gcs.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json,delta)
* `gcs.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `gcs.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
#### Optional Arguments
* `gcs.bigquery.input.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `gcs.bigquery.input.columnnameofcorruptrecord`: Allows renaming the new field having malformed string created by PERMISSIVE mode
* `gcs.bigquery.input.comment`: Sets a single character used for skipping lines beginning with this character. By default it is disabled
* `gcs.bigquery.input.dateformat`: Sets the string that indicates a date format. This applies to date type
* `gcs.bigquery.input.emptyvalue`: Sets the string representation of an empty value
* `gcs.bigquery.input.encoding`: Decodes the CSV files by the given encoding type
* `gcs.bigquery.input.enforceschema`: If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
* `gcs.bigquery.input.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `gcs.bigquery.input.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `gcs.bigquery.input.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `gcs.bigquery.input.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `gcs.bigquery.input.inferschema`: Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
* `gcs.bigquery.input.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `gcs.bigquery.input.locale`: Sets a locale as language tag in IETF BCP 47 format
* `gcs.bigquery.input.maxcharspercolumn`: Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
* `gcs.bigquery.input.maxcolumns`: Defines a hard limit of how many columns a record can have
* `gcs.bigquery.input.mode`: Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
* `gcs.bigquery.input.multiline`: Parse one record, which may span multiple lines, per file
* `gcs.bigquery.input.nanvalue`: Sets the string representation of a non-number value
* `gcs.bigquery.input.nullvalue`: Sets the string representation of a null value
* `gcs.bigquery.input.negativeinf`: Sets the string representation of a negative infinity value
* `gcs.bigquery.input.positiveinf`: Sets the string representation of a positive infinity value
* `gcs.bigquery.input.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `gcs.bigquery.input.samplingratio`: Defines fraction of rows used for schema inferring
* `gcs.bigquery.input.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `gcs.bigquery.input.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `gcs.bigquery.input.timestampntzformat`: Sets the string that indicates a timestamp without timezone format
* `gcs.bigquery.input.unescapedquotehandling`: Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE, RAISE_ERROR

## Usage

```
$ python main.py --template GCSTOBIGQUERY --help

usage: main.py [-h]
               --gcs.bigquery.input.location GCS.BIGQUERY.INPUT.LOCATION
               --gcs.bigquery.output.dataset GCS.BIGQUERY.OUTPUT.DATASET
               --gcs.bigquery.output.table GCS.BIGQUERY.OUTPUT.TABLE
               --gcs.bigquery.input.format {avro,parquet,csv,json,delta}
               [--gcs.bigquery.input.chartoescapequoteescaping GCS.BIGQUERY.INPUT.CHARTOESCAPEQUOTEESCAPING]
               [--gcs.bigquery.input.columnnameofcorruptrecord GCS.BIGQUERY.INPUT.COLUMNNAMEOFCORRUPTRECORD]
               [--gcs.bigquery.input.comment GCS.BIGQUERY.INPUT.COMMENT]
               [--gcs.bigquery.input.dateformat GCS.BIGQUERY.INPUT.DATEFORMAT]
               [--gcs.bigquery.input.emptyvalue GCS.BIGQUERY.INPUT.EMPTYVALUE]
               [--gcs.bigquery.input.encoding GCS.BIGQUERY.INPUT.ENCODING]
               [--gcs.bigquery.input.enforceschema GCS.BIGQUERY.INPUT.ENFORCESCHEMA]
               [--gcs.bigquery.input.escape GCS.BIGQUERY.INPUT.ESCAPE]
               [--gcs.bigquery.input.header GCS.BIGQUERY.INPUT.HEADER]
               [--gcs.bigquery.input.ignoreleadingwhitespace GCS.BIGQUERY.INPUT.IGNORELEADINGWHITESPACE]
               [--gcs.bigquery.input.ignoretrailingwhitespace GCS.BIGQUERY.INPUT.IGNORETRAILINGWHITESPACE]
               [--gcs.bigquery.input.inferschema GCS.BIGQUERY.INPUT.INFERSCHEMA]
               [--gcs.bigquery.input.linesep GCS.BIGQUERY.INPUT.LINESEP]
               [--gcs.bigquery.input.locale GCS.BIGQUERY.INPUT.LOCALE]
               [--gcs.bigquery.input.maxcharspercolumn GCS.BIGQUERY.INPUT.MAXCHARSPERCOLUMN]
               [--gcs.bigquery.input.maxcolumns GCS.BIGQUERY.INPUT.MAXCOLUMNS]
               [--gcs.bigquery.input.mode GCS.BIGQUERY.INPUT.MODE]
               [--gcs.bigquery.input.multiline GCS.BIGQUERY.INPUT.MULTILINE]
               [--gcs.bigquery.input.nanvalue GCS.BIGQUERY.INPUT.NANVALUE]
               [--gcs.bigquery.input.nullvalue GCS.BIGQUERY.INPUT.NULLVALUE]
               [--gcs.bigquery.input.negativeinf GCS.BIGQUERY.INPUT.NEGATIVEINF]
               [--gcs.bigquery.input.positiveinf GCS.BIGQUERY.INPUT.POSITIVEINF]
               [--gcs.bigquery.input.quote GCS.BIGQUERY.INPUT.QUOTE]
               [--gcs.bigquery.input.samplingratio GCS.BIGQUERY.INPUT.SAMPLINGRATIO]
               [--gcs.bigquery.input.sep GCS.BIGQUERY.INPUT.SEP]
               [--gcs.bigquery.input.timestampformat GCS.BIGQUERY.INPUT.TIMESTAMPFORMAT]
               [--gcs.bigquery.input.timestampntzformat GCS.BIGQUERY.INPUT.TIMESTAMPNTZFORMAT]
               [--gcs.bigquery.input.unescapedquotehandling GCS.BIGQUERY.INPUT.UNESCAPEDQUOTEHANDLING]
               --gcs.bigquery.temp.bucket.name GCS.BIGQUERY.TEMP.BUCKET.NAME
               [--gcs.bigquery.output.mode {overwrite,append,ignore,errorifexists}]

options:
  -h, --help            show this help message and exit
  --gcs.bigquery.input.location GCS.BIGQUERY.INPUT.LOCATION
                        Cloud Storage location of the input files
  --gcs.bigquery.output.dataset GCS.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --gcs.bigquery.output.table GCS.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --gcs.bigquery.input.format {avro,parquet,csv,json,delta}
                        Input file format (one of: avro,parquet,csv,json,delta)
  --gcs.bigquery.input.chartoescapequoteescaping GCS.BIGQUERY.INPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --gcs.bigquery.input.columnnameofcorruptrecord GCS.BIGQUERY.INPUT.COLUMNNAMEOFCORRUPTRECORD
                        Allows renaming the new field having malformed string created by PERMISSIVE mode
  --gcs.bigquery.input.comment GCS.BIGQUERY.INPUT.COMMENT
                        Sets a single character used for skipping lines beginning with this character. By default it is disabled
  --gcs.bigquery.input.dateformat GCS.BIGQUERY.INPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --gcs.bigquery.input.emptyvalue GCS.BIGQUERY.INPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --gcs.bigquery.input.encoding GCS.BIGQUERY.INPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --gcs.bigquery.input.enforceschema GCS.BIGQUERY.INPUT.ENFORCESCHEMA
                        If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is
                        set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
  --gcs.bigquery.input.escape GCS.BIGQUERY.INPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --gcs.bigquery.input.header GCS.BIGQUERY.INPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --gcs.bigquery.input.ignoreleadingwhitespace GCS.BIGQUERY.INPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --gcs.bigquery.input.ignoretrailingwhitespace GCS.BIGQUERY.INPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --gcs.bigquery.input.inferschema GCS.BIGQUERY.INPUT.INFERSCHEMA
                        Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
  --gcs.bigquery.input.linesep GCS.BIGQUERY.INPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --gcs.bigquery.input.locale GCS.BIGQUERY.INPUT.LOCALE
                        Sets a locale as language tag in IETF BCP 47 format
  --gcs.bigquery.input.maxcharspercolumn GCS.BIGQUERY.INPUT.MAXCHARSPERCOLUMN
                        Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
  --gcs.bigquery.input.maxcolumns GCS.BIGQUERY.INPUT.MAXCOLUMNS
                        Defines a hard limit of how many columns a record can have
  --gcs.bigquery.input.mode GCS.BIGQUERY.INPUT.MODE
                        Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
  --gcs.bigquery.input.multiline GCS.BIGQUERY.INPUT.MULTILINE
                        Parse one record, which may span multiple lines, per file
  --gcs.bigquery.input.nanvalue GCS.BIGQUERY.INPUT.NANVALUE
                        Sets the string representation of a non-number value
  --gcs.bigquery.input.nullvalue GCS.BIGQUERY.INPUT.NULLVALUE
                        Sets the string representation of a null value
  --gcs.bigquery.input.negativeinf GCS.BIGQUERY.INPUT.NEGATIVEINF
                        Sets the string representation of a negative infinity value
  --gcs.bigquery.input.positiveinf GCS.BIGQUERY.INPUT.POSITIVEINF
                        Sets the string representation of a positive infinity value
  --gcs.bigquery.input.quote GCS.BIGQUERY.INPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --gcs.bigquery.input.samplingratio GCS.BIGQUERY.INPUT.SAMPLINGRATIO
                        Defines fraction of rows used for schema inferring
  --gcs.bigquery.input.sep GCS.BIGQUERY.INPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --gcs.bigquery.input.timestampformat GCS.BIGQUERY.INPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --gcs.bigquery.input.timestampntzformat GCS.BIGQUERY.INPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
  --gcs.bigquery.input.unescapedquotehandling GCS.BIGQUERY.INPUT.UNESCAPEDQUOTEHANDLING
                        Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE,
                        RAISE_ERROR
  --gcs.bigquery.temp.bucket.name GCS.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --gcs.bigquery.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example).  
It also requires [DeltaIO dependencies](https://docs.delta.io/latest/releases.html) to be available in the Dataproc cluster if using delta format.


## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar, <gs://{your_bucket}/delta-core_2.12-1.1.0.jar>"

./bin/start.sh \
-- --template=GCSTOBIGQUERY \
    --gcs.bigquery.input.format="csv" \
    --gcs.bigquery.input.location="<gs://bucket/path>" \
    --gcs.bigquery.input.inferschema="false" \
    --gcs.bigquery.output.dataset="<dataset>" \
    --gcs.bigquery.output.table="<table>" \
    --gcs.bigquery.output.mode=<append|overwrite|ignore|errorifexists>\
    --gcs.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```

# Cloud Storage To Bigtable

Template for reading files from Cloud Storage and writing them to a Bigtable table. It supports reading JSON, CSV, Parquet, Avro and Delta formats.

It uses the Apache HBase Spark Connector to write to Bigtable.

This [tutorial](https://cloud.google.com/dataproc/docs/tutorials/spark-hbase#dataproc_hbase_tutorial_view_code-python) shows how to run a Spark/PySpark job connecting to Bigtable.
However, it focuses in running the job using a Dataproc cluster, and not Dataproc Serverless.
Here in this template, you will notice that there are different configuration steps for the PySpark job to successfully run using Dataproc Serverless, connecting to Bigtable using the HBase interface.

You can also check out the [differences between HBase and Cloud Bigtable](https://cloud.google.com/bigtable/docs/hbase-differences).

## Requirements

1) Configure the [hbase-site.xml](./hbase-site.xml) ([reference](https://cloud.google.com/bigtable/docs/hbase-connecting#creating_the_hbase-sitexml_file)) with your Bigtable instance reference
    - The hbase-site.xml needs to be available in some path of the container image used by Dataproc Serverless.
    - For that, you need to build and host a [customer container image](https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers#submit_a_spark_batch_workload_using_a_custom_container_image) in GCP Container Registry.
      - Add the following layer to the [Dockerfile](./Dockerfile), for it to copy your local hbase-site.xml to the container image (already done):
        ```
        COPY hbase-site.xml /etc/hbase/conf/
        ```
      - Build the [Dockerfile](./Dockerfile), building and pushing it to GCP Container Registry with:
        ```
        IMAGE=gcr.io/<your_project>/<your_custom_image>:<your_version>
        docker build -t "${IMAGE}" .
        docker push "${IMAGE}"
        ```
      - An SPARK_EXTRA_CLASSPATH environment variable should also be set to the same path when submitting the job.
        ```
        (./bin/start.sh ...)
        --container-image="gcr.io/<your_project>/<your_custom_image>:<your_version>"  # image with hbase-site.xml in /etc/hbase/conf/
        --properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/'
        ```

2) Configure the desired HBase catalog json to passed as an argument (table reference and schema)
    - The hbase-catalog.json should be passed using the --gcs.bigtable.hbase.catalog.json
    ```
    (./bin/start.sh ...)
    -- --gcs.bigtable.hbase.catalog.json='''{
                        "table":{"namespace":"default","name":"<table_id>"},
                        "rowkey":"key",
                        "columns":{
                        "key":{"cf":"rowkey", "col":"key", "type":"string"},
                        "name":{"cf":"cf", "col":"name", "type":"string"}
                        }
                    }'''
    ```

3) [Create and manage](https://cloud.google.com/bigtable/docs/managing-tables) your Bigtable table schema, column families, etc, to match the provided HBase catalog.

## Required JAR files

Some HBase and Bigtable dependencies are required to be passed when submitting the job.
These dependencies need to be passed by using the --jars flag, or, in the case of Dataproc Templates, using the JARS environment variable.
Some dependencies (jars) must be downloaded from [MVN Repository](https://mvnrepository.com/) and stored your Cloud Storage bucket (create one to store the dependencies).

- **[Apache HBase Spark Connector](https://mvnrepository.com/artifact/org.apache.hbase.connectors.spark/hbase-spark) dependencies (already mounted in Dataproc Serverless, so you refer to them using file://):**
   - file:///usr/lib/spark/external/hbase-spark-protocol-shaded.jar
   - file:///usr/lib/spark/external/hbase-spark.jar

- **Bigtable dependency:**
  - gs://<your_bucket_to_store_dependencies>/bigtable-hbase-2.x-hadoop-2.3.0.jar
    - Download it using ``` wget https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-2.x-shaded/2.3.0/bigtable-hbase-2.x-shaded-2.3.0.jar```

- **HBase dependencies:**
  - gs://<your_bucket_to_store_dependencies>/hbase-client-2.4.12.jar
      - Download it using ``` wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar```
  - gs://<your_bucket_to_store_dependencies>/hbase-shaded-mapreduce-2.4.12.jar
      - Download it using ``` wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-shaded-mapreduce/2.4.12/hbase-shaded-mapreduce-2.4.12.jar```

It also requires [DeltaIO dependencies](https://docs.delta.io/latest/releases.html) to be available in the Dataproc cluster if using delta format.


## Arguments
* `gcs.bigquery.input.location`: Cloud Storage location of the input files (format: `gs://<bucket>/...`)
* `gcs.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json,delta)
* `gcs.bigtable.hbase.catalog.json`: HBase catalog inline json
#### Optional Arguments
* `gcs.bigtable.input.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `gcs.bigtable.input.columnnameofcorruptrecord`: Allows renaming the new field having malformed string created by PERMISSIVE mode
* `gcs.bigtable.input.comment`: Sets a single character used for skipping lines beginning with this character. By default it is disabled
* `gcs.bigtable.input.dateformat`: Sets the string that indicates a date format. This applies to date type
* `gcs.bigtable.input.emptyvalue`: Sets the string representation of an empty value
* `gcs.bigtable.input.encoding`: Decodes the CSV files by the given encoding type
* `gcs.bigtable.input.enforceschema`: If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
* `gcs.bigtable.input.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `gcs.bigtable.input.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `gcs.bigtable.input.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `gcs.bigtable.input.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `gcs.bigtable.input.inferschema`: Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
* `gcs.bigtable.input.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `gcs.bigtable.input.locale`: Sets a locale as language tag in IETF BCP 47 format
* `gcs.bigtable.input.maxcharspercolumn`: Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
* `gcs.bigtable.input.maxcolumns`: Defines a hard limit of how many columns a record can have
* `gcs.bigtable.input.mode`: Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
* `gcs.bigtable.input.multiline`: Parse one record, which may span multiple lines, per file
* `gcs.bigtable.input.nanvalue`: Sets the string representation of a non-number value
* `gcs.bigtable.input.nullvalue`: Sets the string representation of a null value
* `gcs.bigtable.input.negativeinf`: Sets the string representation of a negative infinity value
* `gcs.bigtable.input.positiveinf`: Sets the string representation of a positive infinity value
* `gcs.bigtable.input.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `gcs.bigtable.input.samplingratio`: Defines fraction of rows used for schema inferring
* `gcs.bigtable.input.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `gcs.bigtable.input.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `gcs.bigtable.input.timestampntzformat`: Sets the string that indicates a timestamp without timezone format
* `gcs.bigtable.input.unescapedquotehandling`: Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE, RAISE_ERROR

## Usage

```
$ python main.py --template GCSTOBIGTABLE --help

usage: main.py [-h]
               --gcs.bigtable.input.location GCS.BIGTABLE.INPUT.LOCATION
               --gcs.bigtable.input.format {avro,parquet,csv,json,delta}
               [--gcs.bigtable.input.chartoescapequoteescaping GCS.BIGTABLE.INPUT.CHARTOESCAPEQUOTEESCAPING]
               [--gcs.bigtable.input.columnnameofcorruptrecord GCS.BIGTABLE.INPUT.COLUMNNAMEOFCORRUPTRECORD]
               [--gcs.bigtable.input.comment GCS.BIGTABLE.INPUT.COMMENT]
               [--gcs.bigtable.input.dateformat GCS.BIGTABLE.INPUT.DATEFORMAT]
               [--gcs.bigtable.input.emptyvalue GCS.BIGTABLE.INPUT.EMPTYVALUE]
               [--gcs.bigtable.input.encoding GCS.BIGTABLE.INPUT.ENCODING]
               [--gcs.bigtable.input.enforceschema GCS.BIGTABLE.INPUT.ENFORCESCHEMA]
               [--gcs.bigtable.input.escape GCS.BIGTABLE.INPUT.ESCAPE]
               [--gcs.bigtable.input.header GCS.BIGTABLE.INPUT.HEADER]
               [--gcs.bigtable.input.ignoreleadingwhitespace GCS.BIGTABLE.INPUT.IGNORELEADINGWHITESPACE]
               [--gcs.bigtable.input.ignoretrailingwhitespace GCS.BIGTABLE.INPUT.IGNORETRAILINGWHITESPACE]
               [--gcs.bigtable.input.inferschema GCS.BIGTABLE.INPUT.INFERSCHEMA]
               [--gcs.bigtable.input.linesep GCS.BIGTABLE.INPUT.LINESEP]
               [--gcs.bigtable.input.locale GCS.BIGTABLE.INPUT.LOCALE]
               [--gcs.bigtable.input.maxcharspercolumn GCS.BIGTABLE.INPUT.MAXCHARSPERCOLUMN]
               [--gcs.bigtable.input.maxcolumns GCS.BIGTABLE.INPUT.MAXCOLUMNS]
               [--gcs.bigtable.input.mode GCS.BIGTABLE.INPUT.MODE]
               [--gcs.bigtable.input.multiline GCS.BIGTABLE.INPUT.MULTILINE]
               [--gcs.bigtable.input.nanvalue GCS.BIGTABLE.INPUT.NANVALUE]
               [--gcs.bigtable.input.nullvalue GCS.BIGTABLE.INPUT.NULLVALUE]
               [--gcs.bigtable.input.negativeinf GCS.BIGTABLE.INPUT.NEGATIVEINF]
               [--gcs.bigtable.input.positiveinf GCS.BIGTABLE.INPUT.POSITIVEINF]
               [--gcs.bigtable.input.quote GCS.BIGTABLE.INPUT.QUOTE]
               [--gcs.bigtable.input.samplingratio GCS.BIGTABLE.INPUT.SAMPLINGRATIO]
               [--gcs.bigtable.input.sep GCS.BIGTABLE.INPUT.SEP]
               [--gcs.bigtable.input.timestampformat GCS.BIGTABLE.INPUT.TIMESTAMPFORMAT]
               [--gcs.bigtable.input.timestampntzformat GCS.BIGTABLE.INPUT.TIMESTAMPNTZFORMAT]
               [--gcs.bigtable.input.unescapedquotehandling GCS.BIGTABLE.INPUT.UNESCAPEDQUOTEHANDLING]
               --gcs.bigtable.hbase.catalog.json GCS.BIGTABLE.HBASE.CATALOG.JSON

options:
  -h, --help            show this help message and exit
  --gcs.bigtable.input.location GCS.BIGTABLE.INPUT.LOCATION
                        Cloud Storage location of the input files
  --gcs.bigtable.input.format {avro,parquet,csv,json,delta}
                        Input file format (one of: avro,parquet,csv,json,delta)
  --gcs.bigtable.input.chartoescapequoteescaping GCS.BIGTABLE.INPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --gcs.bigtable.input.columnnameofcorruptrecord GCS.BIGTABLE.INPUT.COLUMNNAMEOFCORRUPTRECORD
                        Allows renaming the new field having malformed string created by PERMISSIVE mode
  --gcs.bigtable.input.comment GCS.BIGTABLE.INPUT.COMMENT
                        Sets a single character used for skipping lines beginning with this character. By default it is disabled
  --gcs.bigtable.input.dateformat GCS.BIGTABLE.INPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --gcs.bigtable.input.emptyvalue GCS.BIGTABLE.INPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --gcs.bigtable.input.encoding GCS.BIGTABLE.INPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --gcs.bigtable.input.enforceschema GCS.BIGTABLE.INPUT.ENFORCESCHEMA
                        If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is
                        set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
  --gcs.bigtable.input.escape GCS.BIGTABLE.INPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --gcs.bigtable.input.header GCS.BIGTABLE.INPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --gcs.bigtable.input.ignoreleadingwhitespace GCS.BIGTABLE.INPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --gcs.bigtable.input.ignoretrailingwhitespace GCS.BIGTABLE.INPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --gcs.bigtable.input.inferschema GCS.BIGTABLE.INPUT.INFERSCHEMA
                        Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
  --gcs.bigtable.input.linesep GCS.BIGTABLE.INPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --gcs.bigtable.input.locale GCS.BIGTABLE.INPUT.LOCALE
                        Sets a locale as language tag in IETF BCP 47 format
  --gcs.bigtable.input.maxcharspercolumn GCS.BIGTABLE.INPUT.MAXCHARSPERCOLUMN
                        Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
  --gcs.bigtable.input.maxcolumns GCS.BIGTABLE.INPUT.MAXCOLUMNS
                        Defines a hard limit of how many columns a record can have
  --gcs.bigtable.input.mode GCS.BIGTABLE.INPUT.MODE
                        Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
  --gcs.bigtable.input.multiline GCS.BIGTABLE.INPUT.MULTILINE
                        Parse one record, which may span multiple lines, per file
  --gcs.bigtable.input.nanvalue GCS.BIGTABLE.INPUT.NANVALUE
                        Sets the string representation of a non-number value
  --gcs.bigtable.input.nullvalue GCS.BIGTABLE.INPUT.NULLVALUE
                        Sets the string representation of a null value
  --gcs.bigtable.input.negativeinf GCS.BIGTABLE.INPUT.NEGATIVEINF
                        Sets the string representation of a negative infinity value
  --gcs.bigtable.input.positiveinf GCS.BIGTABLE.INPUT.POSITIVEINF
                        Sets the string representation of a positive infinity value
  --gcs.bigtable.input.quote GCS.BIGTABLE.INPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --gcs.bigtable.input.samplingratio GCS.BIGTABLE.INPUT.SAMPLINGRATIO
                        Defines fraction of rows used for schema inferring
  --gcs.bigtable.input.sep GCS.BIGTABLE.INPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --gcs.bigtable.input.timestampformat GCS.BIGTABLE.INPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --gcs.bigtable.input.timestampntzformat GCS.BIGTABLE.INPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
  --gcs.bigtable.input.unescapedquotehandling GCS.BIGTABLE.INPUT.UNESCAPEDQUOTEHANDLING
                        Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE,
                        RAISE_ERROR
  --gcs.bigtable.hbase.catalog.json GCS.BIGTABLE.HBASE.CATALOG.JSON
                        HBase catalog inline json
```

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export JARS="gs://<your_bucket_to_store_dependencies>/bigtable-hbase-2.x-hadoop-2.3.0.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-client-2.4.12.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-shaded-mapreduce-2.4.12.jar, \
             file:///usr/lib/spark/external/hbase-spark-protocol-shaded.jar, \
             file:///usr/lib/spark/external/hbase-spark.jar"

./bin/start.sh \
--container-image="gcr.io/<your_project>/<your_custom_image>:<your_version>" \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/' \ # image with hbase-site.xml in /etc/hbase/conf/
-- --template=GCSTOBIGTABLE \
   --gcs.bigtable.input.format="csv" \
   --gcs.bigtable.input.location="<gs://bucket/path>" \
   --gcs.bigtable.input.header="false" \
   --gcs.bigtable.hbase.catalog.json='''{
                        "table":{"namespace":"default","name":"my_table"},
                        "rowkey":"key",
                        "columns":{
                        "key":{"cf":"rowkey", "col":"key", "type":"string"},
                        "name":{"cf":"cf", "col":"name", "type":"string"}
                        }
                    }'''
```


# Cloud Storage To JDBC

Template for reading files from Cloud Storage and writing them to a JDBC table. It supports reading JSON, CSV, Parquet, Avro and Delta formats.

## Arguments
* `gcs.jdbc.input.format`: Input file format (one of: avro,parquet,csv,json,delta)
* `gcs.jdbc.input.location`: Cloud Storage location of the input files (format: `gs://BUCKET/...`)
* `gcs.jdbc.output.table`: JDBC output table name
* `gcs.jdbc.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `gcs.jdbc.output.driver`: JDBC output driver name
* `gcs.jdbc.batch.size`: JDBC output batch size
* `gcs.jdbc.output.url`: JDBC output URL
#### Optional Arguments
* `gcs.jdbc.input.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `gcs.jdbc.input.columnnameofcorruptrecord`: Allows renaming the new field having malformed string created by PERMISSIVE mode
* `gcs.jdbc.input.comment`: Sets a single character used for skipping lines beginning with this character. By default it is disabled
* `gcs.jdbc.input.dateformat`: Sets the string that indicates a date format. This applies to date type
* `gcs.jdbc.input.emptyvalue`: Sets the string representation of an empty value
* `gcs.jdbc.input.encoding`: Decodes the CSV files by the given encoding type
* `gcs.jdbc.input.enforceschema`: If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
* `gcs.jdbc.input.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `gcs.jdbc.input.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `gcs.jdbc.input.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `gcs.jdbc.input.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `gcs.jdbc.input.inferschema`: Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
* `gcs.jdbc.input.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `gcs.jdbc.input.locale`: Sets a locale as language tag in IETF BCP 47 format
* `gcs.jdbc.input.maxcharspercolumn`: Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
* `gcs.jdbc.input.maxcolumns`: Defines a hard limit of how many columns a record can have
* `gcs.jdbc.input.mode`: Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
* `gcs.jdbc.input.multiline`: Parse one record, which may span multiple lines, per file
* `gcs.jdbc.input.nanvalue`: Sets the string representation of a non-number value
* `gcs.jdbc.input.nullvalue`: Sets the string representation of a null value
* `gcs.jdbc.input.negativeinf`: Sets the string representation of a negative infinity value
* `gcs.jdbc.input.positiveinf`: Sets the string representation of a positive infinity value
* `gcs.jdbc.input.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `gcs.jdbc.input.samplingratio`: Defines fraction of rows used for schema inferring
* `gcs.jdbc.input.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `gcs.jdbc.input.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `gcs.jdbc.input.timestampntzformat`: Sets the string that indicates a timestamp without timezone format
* `gcs.jdbc.input.unescapedquotehandling`: Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE, RAISE_ERROR


## Usage

```
$ python main.py --template GCSTOJDBC --help

usage: main.py [-h]
               --gcs.jdbc.input.location GCS.JDBC.INPUT.LOCATION
               --gcs.jdbc.input.format {avro,parquet,csv,json}
               [--gcs.jdbc.input.chartoescapequoteescaping GCS.JDBC.INPUT.CHARTOESCAPEQUOTEESCAPING]
               [--gcs.jdbc.input.columnnameofcorruptrecord GCS.JDBC.INPUT.COLUMNNAMEOFCORRUPTRECORD]
               [--gcs.jdbc.input.comment GCS.JDBC.INPUT.COMMENT]
               [--gcs.jdbc.input.dateformat GCS.JDBC.INPUT.DATEFORMAT]
               [--gcs.jdbc.input.emptyvalue GCS.JDBC.INPUT.EMPTYVALUE]
               [--gcs.jdbc.input.encoding GCS.JDBC.INPUT.ENCODING]
               [--gcs.jdbc.input.enforceschema GCS.JDBC.INPUT.ENFORCESCHEMA]
               [--gcs.jdbc.input.escape GCS.JDBC.INPUT.ESCAPE]
               [--gcs.jdbc.input.header GCS.JDBC.INPUT.HEADER]
               [--gcs.jdbc.input.ignoreleadingwhitespace GCS.JDBC.INPUT.IGNORELEADINGWHITESPACE]
               [--gcs.jdbc.input.ignoretrailingwhitespace GCS.JDBC.INPUT.IGNORETRAILINGWHITESPACE]
               [--gcs.jdbc.input.inferschema GCS.JDBC.INPUT.INFERSCHEMA]
               [--gcs.jdbc.input.linesep GCS.JDBC.INPUT.LINESEP]
               [--gcs.jdbc.input.locale GCS.JDBC.INPUT.LOCALE]
               [--gcs.jdbc.input.maxcharspercolumn GCS.JDBC.INPUT.MAXCHARSPERCOLUMN]
               [--gcs.jdbc.input.maxcolumns GCS.JDBC.INPUT.MAXCOLUMNS]
               [--gcs.jdbc.input.mode GCS.JDBC.INPUT.MODE]
               [--gcs.jdbc.input.multiline GCS.JDBC.INPUT.MULTILINE]
               [--gcs.jdbc.input.nanvalue GCS.JDBC.INPUT.NANVALUE]
               [--gcs.jdbc.input.nullvalue GCS.JDBC.INPUT.NULLVALUE]
               [--gcs.jdbc.input.negativeinf GCS.JDBC.INPUT.NEGATIVEINF]
               [--gcs.jdbc.input.positiveinf GCS.JDBC.INPUT.POSITIVEINF]
               [--gcs.jdbc.input.quote GCS.JDBC.INPUT.QUOTE]
               [--gcs.jdbc.input.samplingratio GCS.JDBC.INPUT.SAMPLINGRATIO]
               [--gcs.jdbc.input.sep GCS.JDBC.INPUT.SEP]
               [--gcs.jdbc.input.timestampformat GCS.JDBC.INPUT.TIMESTAMPFORMAT]
               [--gcs.jdbc.input.timestampntzformat GCS.JDBC.INPUT.TIMESTAMPNTZFORMAT]
               [--gcs.jdbc.input.unescapedquotehandling GCS.JDBC.INPUT.UNESCAPEDQUOTEHANDLING]
               --gcs.jdbc.output.table GCS.JDBC.OUTPUT.TABLE
               [--gcs.jdbc.output.mode {overwrite,append,ignore,errorifexists}]
               --gcs.jdbc.output.url GCS.JDBC.OUTPUT.URL
               --gcs.jdbc.output.driver GCS.JDBC.OUTPUT.DRIVER
               [--gcs.jdbc.batch.size GCS.JDBC.BATCH.SIZE]
               [--gcs.jdbc.numpartitions GCS.JDBC.NUMPARTITIONS]

options:
  -h, --help            show this help message and exit
  --gcs.jdbc.input.location GCS.JDBC.INPUT.LOCATION
                        Cloud Storage location of the input files
  --gcs.jdbc.input.format {avro,parquet,csv,json,delta}
                        Input file format (one of: avro,parquet,csv,json,delta)
  --gcs.jdbc.input.chartoescapequoteescaping GCS.JDBC.INPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --gcs.jdbc.input.columnnameofcorruptrecord GCS.JDBC.INPUT.COLUMNNAMEOFCORRUPTRECORD
                        Allows renaming the new field having malformed string created by PERMISSIVE mode
  --gcs.jdbc.input.comment GCS.JDBC.INPUT.COMMENT
                        Sets a single character used for skipping lines beginning with this character. By default it is disabled
  --gcs.jdbc.input.dateformat GCS.JDBC.INPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --gcs.jdbc.input.emptyvalue GCS.JDBC.INPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --gcs.jdbc.input.encoding GCS.JDBC.INPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --gcs.jdbc.input.enforceschema GCS.JDBC.INPUT.ENFORCESCHEMA
                        If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is
                        set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
  --gcs.jdbc.input.escape GCS.JDBC.INPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --gcs.jdbc.input.header GCS.JDBC.INPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --gcs.jdbc.input.ignoreleadingwhitespace GCS.JDBC.INPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --gcs.jdbc.input.ignoretrailingwhitespace GCS.JDBC.INPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --gcs.jdbc.input.inferschema GCS.JDBC.INPUT.INFERSCHEMA
                        Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
  --gcs.jdbc.input.linesep GCS.JDBC.INPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --gcs.jdbc.input.locale GCS.JDBC.INPUT.LOCALE
                        Sets a locale as language tag in IETF BCP 47 format
  --gcs.jdbc.input.maxcharspercolumn GCS.JDBC.INPUT.MAXCHARSPERCOLUMN
                        Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
  --gcs.jdbc.input.maxcolumns GCS.JDBC.INPUT.MAXCOLUMNS
                        Defines a hard limit of how many columns a record can have
  --gcs.jdbc.input.mode GCS.JDBC.INPUT.MODE
                        Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
  --gcs.jdbc.input.multiline GCS.JDBC.INPUT.MULTILINE
                        Parse one record, which may span multiple lines, per file
  --gcs.jdbc.input.nanvalue GCS.JDBC.INPUT.NANVALUE
                        Sets the string representation of a non-number value
  --gcs.jdbc.input.nullvalue GCS.JDBC.INPUT.NULLVALUE
                        Sets the string representation of a null value
  --gcs.jdbc.input.negativeinf GCS.JDBC.INPUT.NEGATIVEINF
                        Sets the string representation of a negative infinity value
  --gcs.jdbc.input.positiveinf GCS.JDBC.INPUT.POSITIVEINF
                        Sets the string representation of a positive infinity value
  --gcs.jdbc.input.quote GCS.JDBC.INPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --gcs.jdbc.input.samplingratio GCS.JDBC.INPUT.SAMPLINGRATIO
                        Defines fraction of rows used for schema inferring
  --gcs.jdbc.input.sep GCS.JDBC.INPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --gcs.jdbc.input.timestampformat GCS.JDBC.INPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --gcs.jdbc.input.timestampntzformat GCS.JDBC.INPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
  --gcs.jdbc.input.unescapedquotehandling GCS.JDBC.INPUT.UNESCAPEDQUOTEHANDLING
                        Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE,
                        RAISE_ERROR
  --gcs.jdbc.output.table GCS.JDBC.OUTPUT.TABLE
                        JDBC output table name
  --gcs.jdbc.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --gcs.jdbc.output.url GCS.JDBC.OUTPUT.URL
                        JDBC output URL
  --gcs.jdbc.output.driver GCS.JDBC.OUTPUT.DRIVER
                        JDBC output driver name
  --gcs.jdbc.batch.size GCS.JDBC.BATCH.SIZE
                        JDBC output batch size
  --gcs.jdbc.numpartitions GCS.JDBC.NUMPARTITIONS
                        The maximum number of partitions to be used for parallelism in table writing
```

## Required JAR files

This template requires the JDBC jar file to be available in the Dataproc cluster.
User has to download the required jar file and host it inside a Cloud Storage bucket, so that it could be referred during the execution of code.

wget Command to download JDBC MySQL jar file is as follows :-

```
wget http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.30.tar.gz. -O /tmp/mysql-connector.tar.gz

```

Once the jar file gets downloaded, please upload the file into a Cloud Storage bucket.

It also requires [DeltaIO dependencies](https://docs.delta.io/latest/releases.html) to be available in the Dataproc cluster if using delta format.

## Example submission

```
export JARS=<gcs-bucket-location-containing-jar-file>

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

# Cloud Storage To MongoDB

Template for reading files from Cloud Storage and writing them to a MongoDB Collection. It supports reading JSON, CSV, Parquet, Avro and Delta formats.

## Arguments
* `gcs.mongo.input.format`: Input file format (one of: avro,parquet,csv,json,delta)
* `gcs.mongo.input.location`: Cloud Storage location of the input files (format: `gs://BUCKET/...`)
* `gcs.mongo.output.uri`: MongoDB Output URI for connection
* `gcs.mongo.output.database`: MongoDB Output Database Name
* `gcs.mongo.output.collection`: MongoDB Output Collection Name
* `gcs.mongo.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
#### Optional Arguments
* `gcs.mongo.input.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `gcs.mongo.input.columnnameofcorruptrecord`: Allows renaming the new field having malformed string created by PERMISSIVE mode
* `gcs.mongo.input.comment`: Sets a single character used for skipping lines beginning with this character. By default it is disabled
* `gcs.mongo.input.dateformat`: Sets the string that indicates a date format. This applies to date type
* `gcs.mongo.input.emptyvalue`: Sets the string representation of an empty value
* `gcs.mongo.input.encoding`: Decodes the CSV files by the given encoding type
* `gcs.mongo.input.enforceschema`: If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
* `gcs.mongo.input.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `gcs.mongo.input.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `gcs.mongo.input.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `gcs.mongo.input.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `gcs.mongo.input.inferschema`: Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
* `gcs.mongo.input.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `gcs.mongo.input.locale`: Sets a locale as language tag in IETF BCP 47 format
* `gcs.mongo.input.maxcharspercolumn`: Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
* `gcs.mongo.input.maxcolumns`: Defines a hard limit of how many columns a record can have
* `gcs.mongo.input.mode`: Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
* `gcs.mongo.input.multiline`: Parse one record, which may span multiple lines, per file
* `gcs.mongo.input.nanvalue`: Sets the string representation of a non-number value
* `gcs.mongo.input.nullvalue`: Sets the string representation of a null value
* `gcs.mongo.input.negativeinf`: Sets the string representation of a negative infinity value
* `gcs.mongo.input.positiveinf`: Sets the string representation of a positive infinity value
* `gcs.mongo.input.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `gcs.mongo.input.samplingratio`: Defines fraction of rows used for schema inferring
* `gcs.mongo.input.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `gcs.mongo.input.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `gcs.mongo.input.timestampntzformat`: Sets the string that indicates a timestamp without timezone format
* `gcs.mongo.input.unescapedquotehandling`: Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE, RAISE_ERROR

## Usage

```
$ python main.py --template GCSTOMONGO --help

usage: main.py [-h]
               --gcs.mongo.input.location GCS.MONGO.INPUT.LOCATION
               --gcs.mongo.input.format {avro,parquet,csv,json,delta}
               [--gcs.mongo.input.chartoescapequoteescaping GCS.MONGO.INPUT.CHARTOESCAPEQUOTEESCAPING]
               [--gcs.mongo.input.columnnameofcorruptrecord GCS.MONGO.INPUT.COLUMNNAMEOFCORRUPTRECORD]
               [--gcs.mongo.input.comment GCS.MONGO.INPUT.COMMENT]
               [--gcs.mongo.input.dateformat GCS.MONGO.INPUT.DATEFORMAT]
               [--gcs.mongo.input.emptyvalue GCS.MONGO.INPUT.EMPTYVALUE]
               [--gcs.mongo.input.encoding GCS.MONGO.INPUT.ENCODING]
               [--gcs.mongo.input.enforceschema GCS.MONGO.INPUT.ENFORCESCHEMA]
               [--gcs.mongo.input.escape GCS.MONGO.INPUT.ESCAPE]
               [--gcs.mongo.input.header GCS.MONGO.INPUT.HEADER]
               [--gcs.mongo.input.ignoreleadingwhitespace GCS.MONGO.INPUT.IGNORELEADINGWHITESPACE]
               [--gcs.mongo.input.ignoretrailingwhitespace GCS.MONGO.INPUT.IGNORETRAILINGWHITESPACE]
               [--gcs.mongo.input.inferschema GCS.MONGO.INPUT.INFERSCHEMA]
               [--gcs.mongo.input.linesep GCS.MONGO.INPUT.LINESEP]
               [--gcs.mongo.input.locale GCS.MONGO.INPUT.LOCALE]
               [--gcs.mongo.input.maxcharspercolumn GCS.MONGO.INPUT.MAXCHARSPERCOLUMN]
               [--gcs.mongo.input.maxcolumns GCS.MONGO.INPUT.MAXCOLUMNS]
               [--gcs.mongo.input.mode GCS.MONGO.INPUT.MODE]
               [--gcs.mongo.input.multiline GCS.MONGO.INPUT.MULTILINE]
               [--gcs.mongo.input.nanvalue GCS.MONGO.INPUT.NANVALUE]
               [--gcs.mongo.input.nullvalue GCS.MONGO.INPUT.NULLVALUE]
               [--gcs.mongo.input.negativeinf GCS.MONGO.INPUT.NEGATIVEINF]
               [--gcs.mongo.input.positiveinf GCS.MONGO.INPUT.POSITIVEINF]
               [--gcs.mongo.input.quote GCS.MONGO.INPUT.QUOTE]
               [--gcs.mongo.input.samplingratio GCS.MONGO.INPUT.SAMPLINGRATIO]
               [--gcs.mongo.input.sep GCS.MONGO.INPUT.SEP]
               [--gcs.mongo.input.timestampformat GCS.MONGO.INPUT.TIMESTAMPFORMAT]
               [--gcs.mongo.input.timestampntzformat GCS.MONGO.INPUT.TIMESTAMPNTZFORMAT]
               [--gcs.mongo.input.unescapedquotehandling GCS.MONGO.INPUT.UNESCAPEDQUOTEHANDLING]
               --gcs.mongo.output.uri GCS.MONGO.OUTPUT.URI
               --gcs.mongo.output.database GCS.MONGO.OUTPUT.DATABASE
               --gcs.mongo.output.collection GCS.MONGO.OUTPUT.COLLECTION
               [--gcs.mongo.output.mode {overwrite,append,ignore,errorifexists}]
               [--gcs.mongo.batch.size GCS.MONGO.BATCH.SIZE]

options:
  -h, --help            show this help message and exit
  --gcs.mongo.input.location GCS.MONGO.INPUT.LOCATION
                        Cloud Storage location of the input files
  --gcs.mongo.input.format {avro,parquet,csv,json,delta}
                        Input file format (one of: avro,parquet,csv,json,delta)
  --gcs.mongo.input.chartoescapequoteescaping GCS.MONGO.INPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --gcs.mongo.input.columnnameofcorruptrecord GCS.MONGO.INPUT.COLUMNNAMEOFCORRUPTRECORD
                        Allows renaming the new field having malformed string created by PERMISSIVE mode
  --gcs.mongo.input.comment GCS.MONGO.INPUT.COMMENT
                        Sets a single character used for skipping lines beginning with this character. By default it is disabled
  --gcs.mongo.input.dateformat GCS.MONGO.INPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --gcs.mongo.input.emptyvalue GCS.MONGO.INPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --gcs.mongo.input.encoding GCS.MONGO.INPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --gcs.mongo.input.enforceschema GCS.MONGO.INPUT.ENFORCESCHEMA
                        If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is
                        set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
  --gcs.mongo.input.escape GCS.MONGO.INPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --gcs.mongo.input.header GCS.MONGO.INPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --gcs.mongo.input.ignoreleadingwhitespace GCS.MONGO.INPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --gcs.mongo.input.ignoretrailingwhitespace GCS.MONGO.INPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --gcs.mongo.input.inferschema GCS.MONGO.INPUT.INFERSCHEMA
                        Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
  --gcs.mongo.input.linesep GCS.MONGO.INPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --gcs.mongo.input.locale GCS.MONGO.INPUT.LOCALE
                        Sets a locale as language tag in IETF BCP 47 format
  --gcs.mongo.input.maxcharspercolumn GCS.MONGO.INPUT.MAXCHARSPERCOLUMN
                        Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
  --gcs.mongo.input.maxcolumns GCS.MONGO.INPUT.MAXCOLUMNS
                        Defines a hard limit of how many columns a record can have
  --gcs.mongo.input.mode GCS.MONGO.INPUT.MODE
                        Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
  --gcs.mongo.input.multiline GCS.MONGO.INPUT.MULTILINE
                        Parse one record, which may span multiple lines, per file
  --gcs.mongo.input.nanvalue GCS.MONGO.INPUT.NANVALUE
                        Sets the string representation of a non-number value
  --gcs.mongo.input.nullvalue GCS.MONGO.INPUT.NULLVALUE
                        Sets the string representation of a null value
  --gcs.mongo.input.negativeinf GCS.MONGO.INPUT.NEGATIVEINF
                        Sets the string representation of a negative infinity value
  --gcs.mongo.input.positiveinf GCS.MONGO.INPUT.POSITIVEINF
                        Sets the string representation of a positive infinity value
  --gcs.mongo.input.quote GCS.MONGO.INPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --gcs.mongo.input.samplingratio GCS.MONGO.INPUT.SAMPLINGRATIO
                        Defines fraction of rows used for schema inferring
  --gcs.mongo.input.sep GCS.MONGO.INPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --gcs.mongo.input.timestampformat GCS.MONGO.INPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --gcs.mongo.input.timestampntzformat GCS.MONGO.INPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
  --gcs.mongo.input.unescapedquotehandling GCS.MONGO.INPUT.UNESCAPEDQUOTEHANDLING
                        Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE,
                        RAISE_ERROR
  --gcs.mongo.output.uri GCS.MONGO.OUTPUT.URI
                        GCS MONGO Output Connection Uri
  --gcs.mongo.output.database GCS.MONGO.OUTPUT.DATABASE
                        GCS MONGO Output Database Name
  --gcs.mongo.output.collection GCS.MONGO.OUTPUT.COLLECTION
                        GCS MONGO Output Collection Name
  --gcs.mongo.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --gcs.mongo.batch.size GCS.MONGO.BATCH.SIZE
                        GCS MONGO Output Batch Size
```

## Required JAR files

This template requires the MongoDB-Java Driver jar file to be available in the Dataproc cluster. Aprt from that, MongoDB-Spark connector jar file is also required to Export and Import Dataframe via Spark.
User has to download both the required jar files and host it inside a Cloud Storage bucket, so that it could be referred during the execution of code.
Once the jar file gets downloaded, upload the file into a Cloud Storage bucket.

Wget Command to download these jar files is as follows :-

```
sudo wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/2.4.0/mongo-spark-connector_2.12-2.4.0.jar
sudo wget https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.9.1/mongo-java-driver-3.9.1.jar
```

It also requires [DeltaIO dependencies](https://docs.delta.io/latest/releases.html) to be available in the Dataproc cluster if using delta format.

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export JARS=<gcs-bucket-location-containing-jar-file>

./bin/start.sh \
-- --template=GCSTOMONGO \
    --gcs.mongo.input.format="csv" \
    --gcs.mongo.input.location="<gs://bucket/path>" \
    --gcs.mongo.input.dateformat="dd-MMM-yyyy" \
    --gcs.mongo.output.uri="mongodb://<username>:<password>@<Host_Name>:<Port_Number>" \
    --gcs.mongo.output.database="<Database_Name>" \
    --gcs.mongo.output.collection="<Collection_Name>" \
    --gcs.mongo.batch.size=512 \
    --gcs.mongo.output.mode="<append|overwrite|ignore|errorifexists>"
```


# Text To BigQuery

Template for reading TEXT files from Cloud Storage and writing them to a BigQuery table. It supports reading Text files with compression GZIP, BZIP2, LZ4, DEFLATE, NONE.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments
* `text.bigquery.input.location`: Cloud Storage location of the input text files (format: `gs://BUCKET/...`)
* `text.bigquery.output.dataset`: BigQuery dataset for the output table
* `text.bigquery.output.table`: BigQuery output table name
* `text.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `text.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `text.bigquery.input.compression`: Input file compression format (one of: gzip,bzip4,lz4,deflate,none)
* `text.bigquery.input.delimiter`: Input file delimiter
#### Optional Arguments
* `text.bigquery.input.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `text.bigquery.input.columnnameofcorruptrecord`: Allows renaming the new field having malformed string created by PERMISSIVE mode
* `text.bigquery.input.comment`: Sets a single character used for skipping lines beginning with this character. By default it is disabled
* `text.bigquery.input.dateformat`: Sets the string that indicates a date format. This applies to date type
* `text.bigquery.input.emptyvalue`: Sets the string representation of an empty value
* `text.bigquery.input.encoding`: Decodes the CSV files by the given encoding type
* `text.bigquery.input.enforceschema`: If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
* `text.bigquery.input.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `text.bigquery.input.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `text.bigquery.input.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `text.bigquery.input.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `text.bigquery.input.inferschema`: Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
* `text.bigquery.input.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `text.bigquery.input.locale`: Sets a locale as language tag in IETF BCP 47 format
* `text.bigquery.input.maxcharspercolumn`: Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
* `text.bigquery.input.maxcolumns`: Defines a hard limit of how many columns a record can have
* `text.bigquery.input.mode`: Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
* `text.bigquery.input.multiline`: Parse one record, which may span multiple lines, per file
* `text.bigquery.input.nanvalue`: Sets the string representation of a non-number value
* `text.bigquery.input.nullvalue`: Sets the string representation of a null value
* `text.bigquery.input.negativeinf`: Sets the string representation of a negative infinity value
* `text.bigquery.input.positiveinf`: Sets the string representation of a positive infinity value
* `text.bigquery.input.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `text.bigquery.input.samplingratio`: Defines fraction of rows used for schema inferring
* `text.bigquery.input.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `text.bigquery.input.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `text.bigquery.input.timestampntzformat`: Sets the string that indicates a timestamp without timezone format
* `text.bigquery.input.unescapedquotehandling`: Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE, RAISE_ERROR

## Usage

```
$ python main.py --template TEXTTOBIGQUERY --help

usage: main.py [-h]
               --text.bigquery.input.location TEXT.BIGQUERY.INPUT.LOCATION
               [--text.bigquery.input.chartoescapequoteescaping TEXT.BIGQUERY.INPUT.CHARTOESCAPEQUOTEESCAPING]
               [--text.bigquery.input.columnnameofcorruptrecord TEXT.BIGQUERY.INPUT.COLUMNNAMEOFCORRUPTRECORD]
               [--text.bigquery.input.comment TEXT.BIGQUERY.INPUT.COMMENT]
               [--text.bigquery.input.dateformat TEXT.BIGQUERY.INPUT.DATEFORMAT]
               [--text.bigquery.input.emptyvalue TEXT.BIGQUERY.INPUT.EMPTYVALUE]
               [--text.bigquery.input.encoding TEXT.BIGQUERY.INPUT.ENCODING]
               [--text.bigquery.input.enforceschema TEXT.BIGQUERY.INPUT.ENFORCESCHEMA]
               [--text.bigquery.input.escape TEXT.BIGQUERY.INPUT.ESCAPE]
               [--text.bigquery.input.header TEXT.BIGQUERY.INPUT.HEADER]
               [--text.bigquery.input.ignoreleadingwhitespace TEXT.BIGQUERY.INPUT.IGNORELEADINGWHITESPACE]
               [--text.bigquery.input.ignoretrailingwhitespace TEXT.BIGQUERY.INPUT.IGNORETRAILINGWHITESPACE]
               [--text.bigquery.input.inferschema TEXT.BIGQUERY.INPUT.INFERSCHEMA]
               [--text.bigquery.input.linesep TEXT.BIGQUERY.INPUT.LINESEP]
               [--text.bigquery.input.locale TEXT.BIGQUERY.INPUT.LOCALE]
               [--text.bigquery.input.maxcharspercolumn TEXT.BIGQUERY.INPUT.MAXCHARSPERCOLUMN]
               [--text.bigquery.input.maxcolumns TEXT.BIGQUERY.INPUT.MAXCOLUMNS]
               [--text.bigquery.input.mode TEXT.BIGQUERY.INPUT.MODE]
               [--text.bigquery.input.multiline TEXT.BIGQUERY.INPUT.MULTILINE]
               [--text.bigquery.input.nanvalue TEXT.BIGQUERY.INPUT.NANVALUE]
               [--text.bigquery.input.nullvalue TEXT.BIGQUERY.INPUT.NULLVALUE]
               [--text.bigquery.input.negativeinf TEXT.BIGQUERY.INPUT.NEGATIVEINF]
               [--text.bigquery.input.positiveinf TEXT.BIGQUERY.INPUT.POSITIVEINF]
               [--text.bigquery.input.quote TEXT.BIGQUERY.INPUT.QUOTE]
               [--text.bigquery.input.samplingratio TEXT.BIGQUERY.INPUT.SAMPLINGRATIO]
               [--text.bigquery.input.sep TEXT.BIGQUERY.INPUT.SEP]
               [--text.bigquery.input.timestampformat TEXT.BIGQUERY.INPUT.TIMESTAMPFORMAT]
               [--text.bigquery.input.timestampntzformat TEXT.BIGQUERY.INPUT.TIMESTAMPNTZFORMAT]
               [--text.bigquery.input.unescapedquotehandling TEXT.BIGQUERY.INPUT.UNESCAPEDQUOTEHANDLING]
               --text.bigquery.output.dataset TEXT.BIGQUERY.OUTPUT.DATASET
               --text.bigquery.output.table TEXT.BIGQUERY.OUTPUT.TABLE
               --text.bigquery.temp.bucket.name
               TEXT.BIGQUERY.TEMP.BUCKET.NAME
               [--text.bigquery.output.mode {overwrite,append,ignore,errorifexists}]
               --text.bigquery.input.compression {bzip2,gzip,deflate,lz4,None}
               [--text.bigquery.input.delimiter TEXT.BIGQUERY.INPUT.DELIMITER]

options:
  -h, --help            show this help message and exit
  --text.bigquery.input.location TEXT.BIGQUERY.INPUT.LOCATION
                        Cloud Storage location of the input text files
  --text.bigquery.input.chartoescapequoteescaping TEXT.BIGQUERY.INPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --text.bigquery.input.columnnameofcorruptrecord TEXT.BIGQUERY.INPUT.COLUMNNAMEOFCORRUPTRECORD
                        Allows renaming the new field having malformed string created by PERMISSIVE mode
  --text.bigquery.input.comment TEXT.BIGQUERY.INPUT.COMMENT
                        Sets a single character used for skipping lines beginning with this character. By default it is disabled
  --text.bigquery.input.dateformat TEXT.BIGQUERY.INPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --text.bigquery.input.emptyvalue TEXT.BIGQUERY.INPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --text.bigquery.input.encoding TEXT.BIGQUERY.INPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --text.bigquery.input.enforceschema TEXT.BIGQUERY.INPUT.ENFORCESCHEMA
                        If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is
                        set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
  --text.bigquery.input.escape TEXT.BIGQUERY.INPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --text.bigquery.input.header TEXT.BIGQUERY.INPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --text.bigquery.input.ignoreleadingwhitespace TEXT.BIGQUERY.INPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --text.bigquery.input.ignoretrailingwhitespace TEXT.BIGQUERY.INPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --text.bigquery.input.inferschema TEXT.BIGQUERY.INPUT.INFERSCHEMA
                        Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
  --text.bigquery.input.linesep TEXT.BIGQUERY.INPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --text.bigquery.input.locale TEXT.BIGQUERY.INPUT.LOCALE
                        Sets a locale as language tag in IETF BCP 47 format
  --text.bigquery.input.maxcharspercolumn TEXT.BIGQUERY.INPUT.MAXCHARSPERCOLUMN
                        Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
  --text.bigquery.input.maxcolumns TEXT.BIGQUERY.INPUT.MAXCOLUMNS
                        Defines a hard limit of how many columns a record can have
  --text.bigquery.input.mode TEXT.BIGQUERY.INPUT.MODE
                        Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
  --text.bigquery.input.multiline TEXT.BIGQUERY.INPUT.MULTILINE
                        Parse one record, which may span multiple lines, per file
  --text.bigquery.input.nanvalue TEXT.BIGQUERY.INPUT.NANVALUE
                        Sets the string representation of a non-number value
  --text.bigquery.input.nullvalue TEXT.BIGQUERY.INPUT.NULLVALUE
                        Sets the string representation of a null value
  --text.bigquery.input.negativeinf TEXT.BIGQUERY.INPUT.NEGATIVEINF
                        Sets the string representation of a negative infinity value
  --text.bigquery.input.positiveinf TEXT.BIGQUERY.INPUT.POSITIVEINF
                        Sets the string representation of a positive infinity value
  --text.bigquery.input.quote TEXT.BIGQUERY.INPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --text.bigquery.input.samplingratio TEXT.BIGQUERY.INPUT.SAMPLINGRATIO
                        Defines fraction of rows used for schema inferring
  --text.bigquery.input.sep TEXT.BIGQUERY.INPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --text.bigquery.input.timestampformat TEXT.BIGQUERY.INPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --text.bigquery.input.timestampntzformat TEXT.BIGQUERY.INPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
  --text.bigquery.input.unescapedquotehandling TEXT.BIGQUERY.INPUT.UNESCAPEDQUOTEHANDLING
                        Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE,
                        RAISE_ERROR
  --text.bigquery.output.dataset TEXT.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --text.bigquery.output.table TEXT.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --text.bigquery.temp.bucket.name TEXT.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --text.bigquery.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --text.bigquery.input.compression {bzip2,gzip,deflate,lz4,None}
                        Input file compression format (one of: bzip2,deflate,lz4,gzip,None)
  --text.bigquery.input.delimiter TEXT.BIGQUERY.INPUT.DELIMITER
                        Input column delimiter (example: ",", ";", "|", "/","" )
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
-- --template=TEXTTOBIGQUERY \
    --text.bigquery.input.compression="<gzip|bzip4|lz4|deflate|none>" \
    --text.bigquery.input.delimiter="<delimiter>" \
    --text.bigquery.input.location="<gs://bucket/path>" \
    --text.bigquery.output.dataset="<dataset>" \
    --text.bigquery.output.table="<table>" \
    --text.bigquery.output.mode=<append|overwrite|ignore|errorifexists> \
    --text.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```
# Cloud Storage To Cloud Storage - SQL Transformation

Template for reading files from Cloud Storage, applying data transformations using Spark SQL and then writing the transformed data back to Cloud Storage. It supports reading and writing JSON, CSV, Parquet and Avro formats. Additionally, it can read Delta format.

## Arguments
* `gcs.to.gcs.input.location`: Cloud Storage location of the input files (format: `gs://BUCKET/...`)
* `gcs.to.gcs.input.format`: Input file format (one of: avro,parquet,csv,json,delta)
* `gcs.to.gcs.temp.view.name`: Temp view name for creating a spark sql view on source data.
  This name has to match with the table name that will be used in the SQLquery
* `gcs.to.gcs.sql.query`: SQL query for data transformation. This must use the
                          temp view name as the table to query from.
* `gcs.to.gcs.output.format`: Output file format (one of: avro,parquet,csv,json)
* `gcs.to.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `gcs.to.gcs.output.partition.column`: Partition column name to partition the final output in destination bucket'
* `gcs.to.gcs.output.location`: destination Cloud Storage location
#### Optional Arguments
* `gcs.to.gcs.input.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `gcs.to.gcs.input.columnnameofcorruptrecord`: Allows renaming the new field having malformed string created by PERMISSIVE mode
* `gcs.to.gcs.input.comment`: Sets a single character used for skipping lines beginning with this character. By default it is disabled
* `gcs.to.gcs.input.dateformat`: Sets the string that indicates a date format. This applies to date type
* `gcs.to.gcs.input.emptyvalue`: Sets the string representation of an empty value
* `gcs.to.gcs.input.encoding`: Decodes the CSV files by the given encoding type
* `gcs.to.gcs.input.enforceschema`: If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
* `gcs.to.gcs.input.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `gcs.to.gcs.input.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `gcs.to.gcs.input.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `gcs.to.gcs.input.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `gcs.to.gcs.input.inferschema`: Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
* `gcs.to.gcs.input.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `gcs.to.gcs.input.locale`: Sets a locale as language tag in IETF BCP 47 format
* `gcs.to.gcs.input.maxcharspercolumn`: Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
* `gcs.to.gcs.input.maxcolumns`: Defines a hard limit of how many columns a record can have
* `gcs.to.gcs.input.mode`: Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
* `gcs.to.gcs.input.multiline`: Parse one record, which may span multiple lines, per file
* `gcs.to.gcs.input.nanvalue`: Sets the string representation of a non-number value
* `gcs.to.gcs.input.nullvalue`: Sets the string representation of a null value
* `gcs.to.gcs.input.negativeinf`: Sets the string representation of a negative infinity value
* `gcs.to.gcs.input.positiveinf`: Sets the string representation of a positive infinity value
* `gcs.to.gcs.input.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `gcs.to.gcs.input.samplingratio`: Defines fraction of rows used for schema inferring
* `gcs.to.gcs.input.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `gcs.to.gcs.input.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `gcs.to.gcs.input.timestampntzformat`: Sets the string that indicates a timestamp without timezone format
* `gcs.to.gcs.input.unescapedquotehandling`: Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE, RAISE_ERROR
* `gcs.to.gcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `gcs.to.gcs.output.compression`: None
* `gcs.to.gcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
* `gcs.to.gcs.output.emptyvalue`: Sets the string representation of an empty value
* `gcs.to.gcs.output.encoding`: Decodes the CSV files by the given encoding type
* `gcs.to.gcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `gcs.to.gcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
* `gcs.to.gcs.output.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `gcs.to.gcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `gcs.to.gcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `gcs.to.gcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `gcs.to.gcs.output.nullvalue`: Sets the string representation of a null value
* `gcs.to.gcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `gcs.to.gcs.output.quoteall`: None
* `gcs.to.gcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `gcs.to.gcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `gcs.to.gcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format

## Usage

```
$ python main.py --template GCSTOGCS --help

usage: main.py [-h]
               --gcs.to.gcs.input.location GCS.TO.GCS.INPUT.LOCATION
               --gcs.to.gcs.input.format {avro,parquet,csv,json,delta}
               [--gcs.to.gcs.input.chartoescapequoteescaping GCS.TO.GCS.INPUT.CHARTOESCAPEQUOTEESCAPING]
               [--gcs.to.gcs.input.columnnameofcorruptrecord GCS.TO.GCS.INPUT.COLUMNNAMEOFCORRUPTRECORD]
               [--gcs.to.gcs.input.comment GCS.TO.GCS.INPUT.COMMENT]
               [--gcs.to.gcs.input.dateformat GCS.TO.GCS.INPUT.DATEFORMAT]
               [--gcs.to.gcs.input.emptyvalue GCS.TO.GCS.INPUT.EMPTYVALUE]
               [--gcs.to.gcs.input.encoding GCS.TO.GCS.INPUT.ENCODING]
               [--gcs.to.gcs.input.enforceschema GCS.TO.GCS.INPUT.ENFORCESCHEMA]
               [--gcs.to.gcs.input.escape GCS.TO.GCS.INPUT.ESCAPE]
               [--gcs.to.gcs.input.header GCS.TO.GCS.INPUT.HEADER]
               [--gcs.to.gcs.input.ignoreleadingwhitespace GCS.TO.GCS.INPUT.IGNORELEADINGWHITESPACE]
               [--gcs.to.gcs.input.ignoretrailingwhitespace GCS.TO.GCS.INPUT.IGNORETRAILINGWHITESPACE]
               [--gcs.to.gcs.input.inferschema GCS.TO.GCS.INPUT.INFERSCHEMA]
               [--gcs.to.gcs.input.linesep GCS.TO.GCS.INPUT.LINESEP]
               [--gcs.to.gcs.input.locale GCS.TO.GCS.INPUT.LOCALE]
               [--gcs.to.gcs.input.maxcharspercolumn GCS.TO.GCS.INPUT.MAXCHARSPERCOLUMN]
               [--gcs.to.gcs.input.maxcolumns GCS.TO.GCS.INPUT.MAXCOLUMNS]
               [--gcs.to.gcs.input.mode GCS.TO.GCS.INPUT.MODE]
               [--gcs.to.gcs.input.multiline GCS.TO.GCS.INPUT.MULTILINE]
               [--gcs.to.gcs.input.nanvalue GCS.TO.GCS.INPUT.NANVALUE]
               [--gcs.to.gcs.input.nullvalue GCS.TO.GCS.INPUT.NULLVALUE]
               [--gcs.to.gcs.input.negativeinf GCS.TO.GCS.INPUT.NEGATIVEINF]
               [--gcs.to.gcs.input.positiveinf GCS.TO.GCS.INPUT.POSITIVEINF]
               [--gcs.to.gcs.input.quote GCS.TO.GCS.INPUT.QUOTE]
               [--gcs.to.gcs.input.samplingratio GCS.TO.GCS.INPUT.SAMPLINGRATIO]
               [--gcs.to.gcs.input.sep GCS.TO.GCS.INPUT.SEP]
               [--gcs.to.gcs.input.timestampformat GCS.TO.GCS.INPUT.TIMESTAMPFORMAT]
               [--gcs.to.gcs.input.timestampntzformat GCS.TO.GCS.INPUT.TIMESTAMPNTZFORMAT]
               [--gcs.to.gcs.input.unescapedquotehandling GCS.TO.GCS.INPUT.UNESCAPEDQUOTEHANDLING]
               [--gcs.to.gcs.output.chartoescapequoteescaping GCS.TO.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--gcs.to.gcs.output.compression GCS.TO.GCS.OUTPUT.COMPRESSION]
               [--gcs.to.gcs.output.dateformat GCS.TO.GCS.OUTPUT.DATEFORMAT]
               [--gcs.to.gcs.output.emptyvalue GCS.TO.GCS.OUTPUT.EMPTYVALUE]
               [--gcs.to.gcs.output.encoding GCS.TO.GCS.OUTPUT.ENCODING]
               [--gcs.to.gcs.output.escape GCS.TO.GCS.OUTPUT.ESCAPE]
               [--gcs.to.gcs.output.escapequotes GCS.TO.GCS.OUTPUT.ESCAPEQUOTES]
               [--gcs.to.gcs.output.header GCS.TO.GCS.OUTPUT.HEADER]
               [--gcs.to.gcs.output.ignoreleadingwhitespace GCS.TO.GCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--gcs.to.gcs.output.ignoretrailingwhitespace GCS.TO.GCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--gcs.to.gcs.output.linesep GCS.TO.GCS.OUTPUT.LINESEP]
               [--gcs.to.gcs.output.nullvalue GCS.TO.GCS.OUTPUT.NULLVALUE]
               [--gcs.to.gcs.output.quote GCS.TO.GCS.OUTPUT.QUOTE]
               [--gcs.to.gcs.output.quoteall GCS.TO.GCS.OUTPUT.QUOTEALL]
               [--gcs.to.gcs.output.sep GCS.TO.GCS.OUTPUT.SEP]
               [--gcs.to.gcs.output.timestampformat GCS.TO.GCS.OUTPUT.TIMESTAMPFORMAT]
               [--gcs.to.gcs.output.timestampntzformat GCS.TO.GCS.OUTPUT.TIMESTAMPNTZFORMAT]
               [--gcs.to.gcs.temp.view.name GCS.TO.GCS.TEMP.VIEW.NAME]
               [--gcs.to.gcs.sql.query GCS.TO.GCS.SQL.QUERY]
               [--gcs.to.gcs.output.partition.column GCS.TO.GCS.OUTPUT.PARTITION.COLUMN]
               [--gcs.to.gcs.output.format {avro,parquet,csv,json}]
               [--gcs.to.gcs.output.mode {overwrite,append,ignore,errorifexists}]
               --gcs.to.gcs.output.location GCS.TO.GCS.OUTPUT.LOCATION

options:
  -h, --help            show this help message and exit
  --gcs.to.gcs.input.location GCS.TO.GCS.INPUT.LOCATION
                        Cloud Storage location of the input files
  --gcs.to.gcs.input.format {avro,parquet,csv,json,delta}
                        Cloud Storage input file format (one of: avro,parquet,csv,json,delta)
  --gcs.to.gcs.input.chartoescapequoteescaping GCS.TO.GCS.INPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --gcs.to.gcs.input.columnnameofcorruptrecord GCS.TO.GCS.INPUT.COLUMNNAMEOFCORRUPTRECORD
                        Allows renaming the new field having malformed string created by PERMISSIVE mode
  --gcs.to.gcs.input.comment GCS.TO.GCS.INPUT.COMMENT
                        Sets a single character used for skipping lines beginning with this character. By default it is disabled
  --gcs.to.gcs.input.dateformat GCS.TO.GCS.INPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --gcs.to.gcs.input.emptyvalue GCS.TO.GCS.INPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --gcs.to.gcs.input.encoding GCS.TO.GCS.INPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --gcs.to.gcs.input.enforceschema GCS.TO.GCS.INPUT.ENFORCESCHEMA
                        If it is set to true, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is
                        set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Defaults to True
  --gcs.to.gcs.input.escape GCS.TO.GCS.INPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --gcs.to.gcs.input.header GCS.TO.GCS.INPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --gcs.to.gcs.input.ignoreleadingwhitespace GCS.TO.GCS.INPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --gcs.to.gcs.input.ignoretrailingwhitespace GCS.TO.GCS.INPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --gcs.to.gcs.input.inferschema GCS.TO.GCS.INPUT.INFERSCHEMA
                        Infers the input schema automatically from data. It requires one extra pass over the data. Defaults to True
  --gcs.to.gcs.input.linesep GCS.TO.GCS.INPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --gcs.to.gcs.input.locale GCS.TO.GCS.INPUT.LOCALE
                        Sets a locale as language tag in IETF BCP 47 format
  --gcs.to.gcs.input.maxcharspercolumn GCS.TO.GCS.INPUT.MAXCHARSPERCOLUMN
                        Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
  --gcs.to.gcs.input.maxcolumns GCS.TO.GCS.INPUT.MAXCOLUMNS
                        Defines a hard limit of how many columns a record can have
  --gcs.to.gcs.input.mode GCS.TO.GCS.INPUT.MODE
                        Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes: PERMISSIVE, DROPMALFORMED, FAILFAST
  --gcs.to.gcs.input.multiline GCS.TO.GCS.INPUT.MULTILINE
                        Parse one record, which may span multiple lines, per file
  --gcs.to.gcs.input.nanvalue GCS.TO.GCS.INPUT.NANVALUE
                        Sets the string representation of a non-number value
  --gcs.to.gcs.input.nullvalue GCS.TO.GCS.INPUT.NULLVALUE
                        Sets the string representation of a null value
  --gcs.to.gcs.input.negativeinf GCS.TO.GCS.INPUT.NEGATIVEINF
                        Sets the string representation of a negative infinity value
  --gcs.to.gcs.input.positiveinf GCS.TO.GCS.INPUT.POSITIVEINF
                        Sets the string representation of a positive infinity value
  --gcs.to.gcs.input.quote GCS.TO.GCS.INPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --gcs.to.gcs.input.samplingratio GCS.TO.GCS.INPUT.SAMPLINGRATIO
                        Defines fraction of rows used for schema inferring
  --gcs.to.gcs.input.sep GCS.TO.GCS.INPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --gcs.to.gcs.input.timestampformat GCS.TO.GCS.INPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --gcs.to.gcs.input.timestampntzformat GCS.TO.GCS.INPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
  --gcs.to.gcs.input.unescapedquotehandling GCS.TO.GCS.INPUT.UNESCAPEDQUOTEHANDLING
                        Defines how the CsvParser will handle values with unescaped quotes.Valid values are: STOP_AT_CLOSING_QUOTE, BACK_TO_DELIMITER, STOP_AT_DELIMITER, SKIP_VALUE,
                        RAISE_ERROR
  --gcs.to.gcs.output.chartoescapequoteescaping GCS.TO.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --gcs.to.gcs.output.compression GCS.TO.GCS.OUTPUT.COMPRESSION
  --gcs.to.gcs.output.dateformat GCS.TO.GCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --gcs.to.gcs.output.emptyvalue GCS.TO.GCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --gcs.to.gcs.output.encoding GCS.TO.GCS.OUTPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --gcs.to.gcs.output.escape GCS.TO.GCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --gcs.to.gcs.output.escapequotes GCS.TO.GCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --gcs.to.gcs.output.header GCS.TO.GCS.OUTPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --gcs.to.gcs.output.ignoreleadingwhitespace GCS.TO.GCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --gcs.to.gcs.output.ignoretrailingwhitespace GCS.TO.GCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --gcs.to.gcs.output.linesep GCS.TO.GCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --gcs.to.gcs.output.nullvalue GCS.TO.GCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --gcs.to.gcs.output.quote GCS.TO.GCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --gcs.to.gcs.output.quoteall GCS.TO.GCS.OUTPUT.QUOTEALL
  --gcs.to.gcs.output.sep GCS.TO.GCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --gcs.to.gcs.output.timestampformat GCS.TO.GCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --gcs.to.gcs.output.timestampntzformat GCS.TO.GCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
  --gcs.to.gcs.temp.view.name GCS.TO.GCS.TEMP.VIEW.NAME
                        Temp view name for creating a spark sql view on source data. This name has to match with the table name that will be used in the SQL query
  --gcs.to.gcs.sql.query GCS.TO.GCS.SQL.QUERY
                        SQL query for data transformation. This must use the temp view name as the table to query from.
  --gcs.to.gcs.output.partition.column GCS.TO.GCS.OUTPUT.PARTITION.COLUMN
                        Partition column name to partition the final output in destination bucket
  --gcs.to.gcs.output.format {avro,parquet,csv,json}
                        Output write format (one of: avro,parquet,csv,json)(Defaults to parquet)
  --gcs.to.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --gcs.to.gcs.output.location GCS.TO.GCS.OUTPUT.LOCATION
                        Destination Cloud Storage location
```

## Required JAR files

```

It requires [DeltaIO dependencies](https://docs.delta.io/latest/releases.html) to be available in the Dataproc cluster if using delta format.

```


## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export JARS=<gcs-bucket-location-containing-jar-file>

./bin/start.sh \
-- --template=GCSTOGCS \
    --gcs.to.gcs.input.location="<gs://bucket/path>" \
    --gcs.to.gcs.input.format="avro" \
    --gcs.to.gcs.temp.view.name="temp" \
    --gcs.to.gcs.sql.query="select *, 1 as col from temp" \
    --gcs.to.gcs.output.format="csv" \
    --gcs.to.gcs.output.sep="|" \
    --gcs.to.gcs.output.mode="<append|overwrite|ignore|errorifexists>" \
    --gcs.to.gcs.output.location="<gs://bucket/path>"
```