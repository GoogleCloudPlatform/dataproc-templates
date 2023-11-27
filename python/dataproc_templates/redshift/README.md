# Redshift To Cloud Storage

Template for reading data from Redshift table and writing into files in Google Cloud Storage. It supports reading partition tables and supports writing in JSON, CSV, Parquet and Avro formats.

# Prerequisites

## Required JAR files

These templates requires the jar file to be available in the Dataproc cluster.
User has to download the required jar file and host it inside a Cloud Storage Bucket, so that it could be referred during the execution of code.

* spark-redshift.jar
```
wget https://repo1.maven.org/maven2/io/github/spark-redshift-community/spark-redshift_2.12/5.0.3/spark-redshift_2.12-5.0.3.jar
```
* spark-avro.jar
```
wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.3.0/spark-avro_2.12-3.3.0.jar
```
* RedshiftJDBC.jar
```
wget https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/2.1.0.9/redshift-jdbc42-2.1.0.9.jar
```
* minimal-json.jar
```
wget https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.5/minimal-json-0.9.5.jar
```

Once the jar file gets downloaded, please upload the file into a Cloud Storage Bucket and export the below variable

```
export JARS=<comma-seperated-gcs-bucket-location-containing-jar-file>
```

## Redshift JDBC URL syntax

* Redshift
```
jdbc:redshift://[Redshift Endpoint]:[PORT]/<dbname>?user=<username>&password=<password>
```


## Other important properties

* Required Arguments

    * Temp S3 Location
    ```
    redshifttogcs.s3.tempdir="s3a://[Bucket-Name]/[tempdir-path]"
    ```
    * Redshift IAM Role ARN
    ```
    redshifttogcs.iam.rolearn="arn:aws:iam::Account:role/RoleName"
    ```
    * S3 Access Key
    ```
    redshifttogcs.s3.accesskey="xxxxxxxx"
    ```
    * S3 Secret Key
    ```
    redshifttogcs.s3.secretkey="xxxxxxxx"
    ```

* You can specify the source table name. Example,

```
redshifttogcs.input.table="employees"
```

## Arguments
* `redshifttogcs.input.url`: Redshift JDBC input URL
* `redshifttogcs.s3.tempdir`: S3 temporary bucket location
* `redshifttogcs.input.table`: Redshift input table name
* `redshifttogcs.output.location`: Cloud Storage location for output files (format: `gs://BUCKET/...`)
* `redshifttogcs.output.format`: Output file format (one of: avro,parquet,csv,json)
* `redshifttogcs.iam.rolearn` : IAM Role with S3 Access
* `redshifttogcs.s3.accesskey` : AWS Access Key for S3 Access
* `redshifttogcs.s3.secretkey` : AWS Secret Key for S3 Access
* `redshifttogcs.output.mode` (Optional): Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
* `redshifttogcs.output.partitioncolumn` (Optional): Output partition column name
#### Optional Arguments
* `redshifttogcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `redshifttogcs.output.compression`: None
* `redshifttogcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
* `redshifttogcs.output.emptyvalue`: Sets the string representation of an empty value
* `redshifttogcs.output.encoding`: Decodes the CSV files by the given encoding type
* `redshifttogcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `redshifttogcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
* `redshifttogcs.output.header`: Uses the first line of CSV file as names of columns. Defaults to True
* `redshifttogcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `redshifttogcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `redshifttogcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `redshifttogcs.output.nullvalue`: Sets the string representation of a null value
* `redshifttogcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
* `redshifttogcs.output.quoteall`: None
* `redshifttogcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `redshifttogcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `redshifttogcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format

## Usage

```
$ python main.py --template REDSHIFTTOGCS --help

usage: main.py [-h]
               --redshifttogcs.input.url REDSHIFTTOGCS.INPUT.URL
               --redshifttogcs.input.table REDSHIFTTOGCS.INPUT.TABLE
               --redshifttogcs.s3.tempdir REDSHIFTTOGCS.S3.TEMPDIR
               --redshifttogcs.iam.rolearn REDSHIFTTOGCS.IAM.ROLEARN
               --redshifttogcs.s3.accesskey REDSHIFTTOGCS.S3.ACCESSKEY
               --redshifttogcs.s3.secretkey REDSHIFTTOGCS.S3.SECRETKEY
               --redshifttogcs.output.location REDSHIFTTOGCS.OUTPUT.LOCATION
               --redshifttogcs.output.format {avro,parquet,csv,json}
               [--redshifttogcs.output.mode {overwrite,append,ignore,errorifexists}]
               [--redshifttogcs.output.partitioncolumn REDSHIFTTOGCS.OUTPUT.PARTITIONCOLUMN]
               [--redshifttogcs.output.chartoescapequoteescaping REDSHIFTTOGCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--redshifttogcs.output.compression REDSHIFTTOGCS.OUTPUT.COMPRESSION]
               [--redshifttogcs.output.dateformat REDSHIFTTOGCS.OUTPUT.DATEFORMAT]
               [--redshifttogcs.output.emptyvalue REDSHIFTTOGCS.OUTPUT.EMPTYVALUE]
               [--redshifttogcs.output.encoding REDSHIFTTOGCS.OUTPUT.ENCODING]
               [--redshifttogcs.output.escape REDSHIFTTOGCS.OUTPUT.ESCAPE]
               [--redshifttogcs.output.escapequotes REDSHIFTTOGCS.OUTPUT.ESCAPEQUOTES]
               [--redshifttogcs.output.header REDSHIFTTOGCS.OUTPUT.HEADER]
               [--redshifttogcs.output.ignoreleadingwhitespace REDSHIFTTOGCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--redshifttogcs.output.ignoretrailingwhitespace REDSHIFTTOGCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--redshifttogcs.output.linesep REDSHIFTTOGCS.OUTPUT.LINESEP]
               [--redshifttogcs.output.nullvalue REDSHIFTTOGCS.OUTPUT.NULLVALUE]
               [--redshifttogcs.output.quote REDSHIFTTOGCS.OUTPUT.QUOTE]
               [--redshifttogcs.output.quoteall REDSHIFTTOGCS.OUTPUT.QUOTEALL]
               [--redshifttogcs.output.sep REDSHIFTTOGCS.OUTPUT.SEP]
               [--redshifttogcs.output.timestampformat REDSHIFTTOGCS.OUTPUT.TIMESTAMPFORMAT]
               [--redshifttogcs.output.timestampntzformat REDSHIFTTOGCS.OUTPUT.TIMESTAMPNTZFORMAT]

options:
  -h, --help            show this help message and exit
  --redshifttogcs.input.url REDSHIFTTOGCS.INPUT.URL
                        REDSHIFT input URL
  --redshifttogcs.input.table REDSHIFTTOGCS.INPUT.TABLE
                        REDSHIFT input table name
  --redshifttogcs.s3.tempdir REDSHIFTTOGCS.S3.TEMPDIR
                        REDSHIFT S3 temporary bucket location s3a://bucket/path
  --redshifttogcs.iam.rolearn REDSHIFTTOGCS.IAM.ROLEARN
                        REDSHIFT IAM Role with S3 Access
  --redshifttogcs.s3.accesskey REDSHIFTTOGCS.S3.ACCESSKEY
                        AWS Access Keys which allow access to S3
  --redshifttogcs.s3.secretkey REDSHIFTTOGCS.S3.SECRETKEY
                        AWS Secret Keys which allow access to S3
  --redshifttogcs.output.location REDSHIFTTOGCS.OUTPUT.LOCATION
                        Cloud Storage location for output files
  --redshifttogcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --redshifttogcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --redshifttogcs.output.partitioncolumn REDSHIFTTOGCS.OUTPUT.PARTITIONCOLUMN
                        Cloud Storage partition column name
  --redshifttogcs.output.chartoescapequoteescaping REDSHIFTTOGCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --redshifttogcs.output.compression REDSHIFTTOGCS.OUTPUT.COMPRESSION
  --redshifttogcs.output.dateformat REDSHIFTTOGCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --redshifttogcs.output.emptyvalue REDSHIFTTOGCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --redshifttogcs.output.encoding REDSHIFTTOGCS.OUTPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --redshifttogcs.output.escape REDSHIFTTOGCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --redshifttogcs.output.escapequotes REDSHIFTTOGCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --redshifttogcs.output.header REDSHIFTTOGCS.OUTPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --redshifttogcs.output.ignoreleadingwhitespace REDSHIFTTOGCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --redshifttogcs.output.ignoretrailingwhitespace REDSHIFTTOGCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --redshifttogcs.output.linesep REDSHIFTTOGCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --redshifttogcs.output.nullvalue REDSHIFTTOGCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --redshifttogcs.output.quote REDSHIFTTOGCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --redshifttogcs.output.quoteall REDSHIFTTOGCS.OUTPUT.QUOTEALL
  --redshifttogcs.output.sep REDSHIFTTOGCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --redshifttogcs.output.timestampformat REDSHIFTTOGCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --redshifttogcs.output.timestampntzformat REDSHIFTTOGCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
```

## General execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs staging location>
export SUBNET=<subnet>
export JARS="<gcs_path_to_jdbc_jar_files>/spark-redshift_2.12-5.0.3.jar,<gcs_path_to_jdbc_jar_files>/redshift-jdbc42-2.1.0.9.jar,<gcs_path_to_jdbc_jar_files>/minimal-json-0.9.5.jar"

./bin/start.sh \
-- --template=REDSHIFTTOGCS \
--redshifttogcs.input.url="jdbc:redshift://[Redshift Endpoint]:[PORT]/<dbname>?user=<username>&password=<password>" \
--redshifttogcs.s3.tempdir="s3a://bucket-name/temp" \
--redshifttogcs.input.table="table-name" \
--redshifttogcs.iam.rolearn="arn:aws:iam::xxxxxxxx:role/Redshift-S3-Role" \
--redshifttogcs.s3.accesskey="xxxxxxxx" \
--redshifttogcs.s3.secretkey="xxxxxxxx" \
--redshifttogcs.output.location="gs://bucket" \
--redshifttogcs.output.mode=<optional-write-mode> \
--redshifttogcs.output.format=<output-write-format> \
--redshifttogcs.output.partitioncolumn=<optional-output-partition-column-name>
```

## Example execution:

```
export GCP_PROJECT=my-gcp-proj
export REGION=us-central1
export GCS_STAGING_LOCATION=gs://my-gcp-proj/staging
export SUBNET=projects/my-gcp-proj/regions/us-central1/subnetworks/default
export JARS="gs://my-gcp-proj/jars/spark-redshift_2.12-5.0.3.jar,gs://my-gcp-proj/jars/redshift-jdbc42-2.1.0.9.jar,gs://my-gcp-proj/jars/minimal-json-0.9.5.jar"
```
```
./bin/start.sh \
-- --template=REDSHIFTTOGCS \
--redshifttogcs.input.url="jdbc:redshift://cluster-123.xxxxxxxx.region.redshift.amazonaws.com:5439/dev?user=user1&password=password1" \
--redshifttogcs.s3.tempdir="s3a://bucket-name/temp" \
--redshifttogcs.input.table="employee" \
--redshifttogcs.iam.rolearn="arn:aws:iam::xxxxxxxx:role/Redshift-S3-Role" \
--redshifttogcs.s3.accesskey="xxxxxxxx" \
--redshifttogcs.s3.secretkey="xxxxxxxx" \
--redshifttogcs.output.location="gs://output_bucket/output/" \
--redshifttogcs.output.mode="overwrite" \
--redshifttogcs.output.format="csv" \
--redshifttogcs.output.partitioncolumn="department_id"
```


