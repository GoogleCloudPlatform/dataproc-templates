## 1. Snowflake To GCS

Template for reading data from a Snowflake table or custom query and writing to Google Cloud Storage. It supports writing JSON, CSV, Parquet and Avro formats.

General Execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<gcp-project-region>
export GCS_STAGING_LOCATION=<gcs-bucket-staging-folder-path>
export SUBNET=<gcp-project-dataproc-clusters-subnet>
export JARS="<gcs-path-to-snowflake-spark-jar>,<gcs-path-to-snowflake-jdbc-jar>"

bin/start.sh \
-- --template=SNOWFLAKETOGCS \
    --snowflake.to.gcs.sf.url=<snowflake-account-url> \
    --snowflake.to.gcs.sf.user=<snowflake-user> \
    --snowflake.to.gcs.sf.password=<snowflake-user-password> \
    --snowflake.to.gcs.sf.database=<snowflake-database> \
    --snowflake.to.gcs.sf.schema=<snowflake-schema> \
    --snowflake.to.gcs.sf.warehouse=<snowflake-warehouse> \
    --snowflake.to.gcs.sf.table=<snowflake-table> \
    --snowflake.to.gcs.output.location=<gcs-output-location> \
    --snowflake.to.gcs.output.format=<csv|avro|orc|json|parquet> \
    --snowflake.to.gcs.output.mode=<Overwrite|ErrorIfExists|Append|Ignore> \
    --snowflake.to.gcs.partition.column=<gcs-output-partitionby-columnname> \
    --snowflake.gcs.sf.autopushdown=<on|off>
```

### Usage

```
$ python main.py --template SNOWFLAKETOGCS --help

usage: main.py [-h] --snowflake.to.gcs.sf.url SNOWFLAKE.TO.GCS.SF.URL --snowflake.to.gcs.sf.user SNOWFLAKE.TO.GCS.SF.USER --snowflake.to.gcs.sf.password SNOWFLAKE.TO.GCS.SF.PASSWORD [--snowflake.to.gcs.sf.database SNOWFLAKE.TO.GCS.SF.DATABASE]
               [--snowflake.to.gcs.sf.warehouse SNOWFLAKE.TO.GCS.SF.WAREHOUSE] [--snowflake.to.gcs.sf.autopushdown SNOWFLAKE.TO.GCS.SF.AUTOPUSHDOWN] [--snowflake.to.gcs.sf.schema SNOWFLAKE.TO.GCS.SF.SCHEMA] [--snowflake.to.gcs.sf.table SNOWFLAKE.TO.GCS.SF.TABLE]
               [--snowflake.to.gcs.sf.query SNOWFLAKE.TO.GCS.SF.QUERY] --snowflake.to.gcs.output.location SNOWFLAKE.TO.GCS.OUTPUT.LOCATION [--snowflake.to.gcs.output.mode {overwrite,append,ignore,errorifexists}] [--snowflake.to.gcs.output.format {avro,parquet,csv,json}]
               [--snowflake.to.gcs.partition.column SNOWFLAKE.TO.GCS.PARTITION.COLUMN] [--snowflake.gcs.output.linesep SNOWFLAKE.GCS.OUTPUT.LINESEP] [--snowflake.gcs.output.chartoescapequoteescaping SNOWFLAKE.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--snowflake.gcs.output.escape SNOWFLAKE.GCS.OUTPUT.ESCAPE] [--snowflake.gcs.output.escapequotes SNOWFLAKE.GCS.OUTPUT.ESCAPEQUOTES] [--snowflake.gcs.output.timestampntzformat SNOWFLAKE.GCS.OUTPUT.TIMESTAMPNTZFORMAT]
               [--snowflake.gcs.output.compression SNOWFLAKE.GCS.OUTPUT.COMPRESSION] [--snowflake.gcs.output.encoding SNOWFLAKE.GCS.OUTPUT.ENCODING] [--snowflake.gcs.output.quoteall SNOWFLAKE.GCS.OUTPUT.QUOTEALL] [--snowflake.gcs.output.emptyvalue SNOWFLAKE.GCS.OUTPUT.EMPTYVALUE]
               [--snowflake.gcs.output.header SNOWFLAKE.GCS.OUTPUT.HEADER] [--snowflake.gcs.output.sep SNOWFLAKE.GCS.OUTPUT.SEP] [--snowflake.gcs.output.timestampformat SNOWFLAKE.GCS.OUTPUT.TIMESTAMPFORMAT]
               [--snowflake.gcs.output.ignoretrailingwhitespace SNOWFLAKE.GCS.OUTPUT.IGNORETRAILINGWHITESPACE] [--snowflake.gcs.output.dateformat SNOWFLAKE.GCS.OUTPUT.DATEFORMAT] [--snowflake.gcs.output.ignoreleadingwhitespace SNOWFLAKE.GCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--snowflake.gcs.output.nullvalue SNOWFLAKE.GCS.OUTPUT.NULLVALUE] [--snowflake.gcs.output.quote SNOWFLAKE.GCS.OUTPUT.QUOTE]

optional arguments:
  -h, --help            show this help message and exit
  --snowflake.to.gcs.sf.url SNOWFLAKE.TO.GCS.SF.URL
                        Snowflake connection URL
  --snowflake.to.gcs.sf.user SNOWFLAKE.TO.GCS.SF.USER
                        Snowflake user name
  --snowflake.to.gcs.sf.password SNOWFLAKE.TO.GCS.SF.PASSWORD
                        Snowflake password
  --snowflake.to.gcs.sf.database SNOWFLAKE.TO.GCS.SF.DATABASE
                        Snowflake database name
  --snowflake.to.gcs.sf.warehouse SNOWFLAKE.TO.GCS.SF.WAREHOUSE
                        Snowflake datawarehouse name
  --snowflake.to.gcs.sf.autopushdown SNOWFLAKE.TO.GCS.SF.AUTOPUSHDOWN
                        Snowflake Autopushdown (on|off)
  --snowflake.to.gcs.sf.schema SNOWFLAKE.TO.GCS.SF.SCHEMA
                        Snowflake Schema, the source table belongs to
  --snowflake.to.gcs.sf.table SNOWFLAKE.TO.GCS.SF.TABLE
                        Snowflake table name
  --snowflake.to.gcs.sf.query SNOWFLAKE.TO.GCS.SF.QUERY
                        Query to be executed on Snowflake to fetch the desired dataset for migration
  --snowflake.to.gcs.output.location SNOWFLAKE.TO.GCS.OUTPUT.LOCATION
                        Cloud Storage output location where the migrated data will be placed
  --snowflake.to.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --snowflake.to.gcs.output.format {avro,parquet,csv,json}
                        Output write format (one of: avro,parquet,csv,json)(Defaults to csv)
  --snowflake.to.gcs.partition.column SNOWFLAKE.TO.GCS.PARTITION.COLUMN
                        Column name to partition data by, in Cloud Storage bucket
  --snowflake.gcs.output.linesep SNOWFLAKE.GCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --snowflake.gcs.output.chartoescapequoteescaping SNOWFLAKE.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
  --snowflake.gcs.output.escape SNOWFLAKE.GCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --snowflake.gcs.output.escapequotes SNOWFLAKE.GCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --snowflake.gcs.output.timestampntzformat SNOWFLAKE.GCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
  --snowflake.gcs.output.compression SNOWFLAKE.GCS.OUTPUT.COMPRESSION
  --snowflake.gcs.output.encoding SNOWFLAKE.GCS.OUTPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --snowflake.gcs.output.quoteall SNOWFLAKE.GCS.OUTPUT.QUOTEALL
  --snowflake.gcs.output.emptyvalue SNOWFLAKE.GCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --snowflake.gcs.output.header SNOWFLAKE.GCS.OUTPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --snowflake.gcs.output.sep SNOWFLAKE.GCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --snowflake.gcs.output.timestampformat SNOWFLAKE.GCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --snowflake.gcs.output.ignoretrailingwhitespace SNOWFLAKE.GCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --snowflake.gcs.output.dateformat SNOWFLAKE.GCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --snowflake.gcs.output.ignoreleadingwhitespace SNOWFLAKE.GCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --snowflake.gcs.output.nullvalue SNOWFLAKE.GCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --snowflake.gcs.output.quote SNOWFLAKE.GCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not null but an empty string
```
**Note:** Make sure that either `snowflake.to.gcs.sf.query` OR `snowflake.to.gcs.sf.database`, `snowflake.to.gcs.sf.schema` and `snowflake.to.gcs.sf.table` are provided.

### Important properties

* Snowflake account URL. Format: <account-identifier>.snowflakecomputing.com
* Usage of `snowflake.to.gcs.sf.autopushdown`
    * This property introduces advanced optimization capabilities for better performance by allowing large and complex Spark logical plans to be translated and pushed down to Snowflake, instead of being processed in spark. This means, Snowflake would do most of the heavy lifting, by leveraging its performance efficiencies.
        ```
        --snowflake.to.gcs.sf.autopushdown="off"
        ```
    Note: The default behaviour of pushdown is enabled with Spark-Snowflake connector.

    To read more this property refer [Snowflake Docs: Overview of the Spark Connector](https://docs.snowflake.com/en/user-guide/spark-connector-overview.html#query-pushdown)

* Usage of `snowflake.to.gcs.sf.warehouse`
    * The Snowflake warehouse to use.
        ```
        --snowflake.to.gcs.sf.warehouse="dwh"
        ```
    Note: If not specified explicitly, it will take the default virtual warehouse configured at Snowflake.


* Usage of `snowflake.to.gcs.sf.database`, `snowflake.to.gcs.sf.schema`, `snowflake.to.gcs.sf.table` and `snowflake.to.gcs.sf.query`
    * Provide the database, schema, table name OR an equivalent select query with the fully-qualified table name.
        ```
        --snowflake.to.gcs.sf.database="SNOWFLAKE_SAMPLE_DATA" \
        --snowflake.to.gcs.sf.schema="TPCDS_SF100TCL" \
        --snowflake.to.gcs.sf.table="CALL_CENTER"
        ```
      OR
        ```
        --snowflake.to.gcs.sf.query="SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CALL_CENTER"
        ```
        NOTE: In case of `snowflake.to.gcs.sf.query`, if the query contains joins on multiple tables from different schemas, ensure all the schemas are mentioned within the query.

### JARS Required

1. Snowflake Connector for Spark : [Maven Repo Download Link](https://mvnrepository.com/artifact/net.snowflake/spark-snowflake)
2. Snowflake JDBC Driver : [Maven Repo Download Link](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc) Please ensure that jdbc driver version is compatible with the snowflake-spark connector version.

Download the above mentioned jars and place them in a GCS bucket.

### Example submission
```
export GCP_PROJECT="sample-project"
export REGION="us-central1"
export SUBNET="default"
export GCS_STAGING_LOCATION="gs://test-bucket"
export JARS="gs:test_bucket/spark-snowflake_2.12-2.10.0-spark_3.1.jar,gs://test_bucket/dependencies/snowflake-jdbc-3.13.14.jar"
bin/start.sh \
-- \
-- --template=SNOWFLAKETOGCS \
--snowflake.to.gcs.sf.url="https://yqnnxfk.snowflakecomputing.com" \
--snowflake.to.gcs.sf.user="test" \
--snowflake.to.gcs.sf.password="pwd1234" \
--snowflake.to.gcs.sf.database="SNOWFLAKE_SAMPLE_DATA" \
--snowflake.to.gcs.sf.schema="TPCDS_SF100TCL" \
--snowflake.to.gcs.sf.table="CALL_CENTER" \
--snowflake.to.gcs.sf.autopushdown="off" \
--snowflake.to.gcs.output.location="gs://test-bucket/snowflake" \
--snowflake.to.gcs.output.format="avro" \
--snowflake.to.gcs.partition.column="CC_CALL_CENTER_SK"
```