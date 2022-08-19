## 1. Snowflake To GCS

General Execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<gcp-project-region>
export GCS_STAGING_LOCATION=<gcs-bucket-staging-folder-path>
export SUBNET=<gcp-project-dataproc-clusters-subnet>
bin/start.sh \
-- \
--template=SNOWFLAKETOGCS \
--snowflake.to.gcs.sf.url=<snowflake-account-url> \
--snowflake.to.gcs.sf.user=<snowflake-user> \
--snowflake.to.gcs.sf.password=<snowflake-user-password> \
--snowflake.to.gcs.sf.database=<snowflake-database> \
--snowflake.to.gcs.sf.schema=<snowflake-schema> \
--snowflake.to.gcs.sf.warehouse=<snowflake-warehouse> \
--snowflake.to.gcs.sf.query=<snowflake-select-query> \
--snowflake.to.gcs.output.location=<gcs-output-location> \
--snowflake.to.gcs.output.format=<csv|avro|orc|json|parquet> \
--snowflake.to.gcs.output.mode=<Overwrite|ErrorIfExists|Append|Ignore> \
--snowflake.to.gcs.partition.column=<gcs-output-partitionby-columnname> \
--snowflake.gcs.sf.autopushdown=<on|off>
```

### Configurable Parameters
Following properties are available in commandline (`python main.py --template JDBCTOJDBC --help`):

```
# Mandatory Parameter: Snowflake account URL. Format: <account-identifier>.snowflakecomputing.com
snowflake.to.gcs.sf.url

# Mandatory Parameter: Snowflake username
snowflake.to.gcs.sf.user

# Mandatory Parameter: Snowflake user password
snowflake.to.gcs.sf.password

# Mandatory Parameter: Snowflake database name
snowflake.to.gcs.sf.database

# Optional Parameter: Snowflake schema name
snowflake.to.gcs.sf.schema
Note: Schema name is mandatory when you use snowflake.to.gcs.sf.table property. Incase of snowflake.to.gcs.sf.query, if the query contains joins on multiple tables from different schemas, make sure to mention all the schemas within the query. As for the snowflake.to.gcs.sf.schema property in this case, you can either not use it at all or provide name of one of the schemas being used in the query.

# Optional Parameter: Snowflake warehouse
snowflake.to.gcs.sf.warehouse

# Optional Parameter: Snowflake query pushdown feature
snowflake.to.gcs.sf.autopushdown
Note: If not specified explicitly through execution command, the default value is on.

# Optional Parameter: Snowflake input table
snowflake.to.gcs.sf.table
Note: Either one of the template properties snowflake.to.gcs.sf.table and snowflake.to.gcs.sf.query must be provided.

# Optional Parameter: Snowflake select query
snowflake.to.gcs.sf.query
Note: Either one of the template properties snowflake.to.gcs.sf.table and snowflake.to.gcs.sf.query must be provided.

# Mandatory Parameter: GCS output location. Format: gs://<bucket-name>/<dir>
snowflake.to.gcs.output.location

# Optional Parameter: GCS ouput file format. Accepted values: csv, avro, orc, json or parquet
snowflake.to.gcs.output.format
Note: If not specified explicitly through execution command, the default value is csv.

# Optional property: GCS ouput write mode. Accepted values: Overwrite, ErrorIfExists, Append or Ignore
snowflake.to.gcs.output.mode
Note: If not specified explicitly through execution command, the default value is Append.

# Optional property: GCS output data partiton by column name
snowflake.to.gcs.partition.column
```

### Important properties

* Usage of `snowflake.to.gcs.sf.autopushdown`
    * This property introduces advanced optimization capabilities for better performance by allowing large and complex Spark logical plans to be translated and pushed down to Snowflake, instead of being processed in spark. This means, Snowflake would do most of the heavy lifting, by leveraging its performance efficiencies.
        ```
        --snowflake.to.gcs.sf.autopushdown off
        ```
    Note: The default behaviour of pushdown is enabled with Spark-Snowflake connector.

    To read more this property refer [Snowflake Docs: Overview of the Spark Connector](https://docs.snowflake.com/en/user-guide/spark-connector-overview.html#query-pushdown)

* Usage of `snowflake.to.gcs.sf.warehouse`
    * The Snowflake warehouse to use.
        ```
        --snowflake.to.gcs.sf.warehouse dwh
        ```
    Note: If not specified explicitly, it will take the default virtual warehouse configured at Snowflake.

* Usage of `snowflake.to.gcs.sf.table` and `snowflake.to.gcs.sf.query`
    * Provide the table name or an equivalent select query.
      
      Note: Only one of the below properties should be provided through the execution command.
        ```
        --snowflake.to.gcs.sf.table="EMPLOYEE_GEO"
        ```
        NOTE: Schema name is mandatory when you use snowflake.to.gcs.sf.table property. 

        ```
        --snowflake.to.gcs.sf.query="SELECT ED.EMP_NAME, CD.COUNTRY FROM EMP.EMPLOYEE_DETAILS AS ED INNER JOIN GEO.COUNTRY_DETAILS AS CD ON ED.C_ID = CD.C_ID"
        ```
        NOTE: Incase of snowflake.to.gcs.sf.query, if the query contains joins on multiple tables from different schemas, ensure all the schemas are mentioned within the query. As for the snowflake.to.gcs.sf.schema property in this case, you can either not use it at all or provide name of one of the schemas being used in the query.


### Example submission
```
export GCP_PROJECT="sample-project"
export REGION="us-central1"
export SUBNET="default"
export GCS_STAGING_LOCATION="gs://test-bucket"
bin/start.sh \
-- \
-- --template=SNOWFLAKETOGCS \
--snowflake.to.gcs.sf.url="https://yqnnxfk.snowflakecomputing.com" \
--snowflake.to.gcs.sf.user="test" \
--snowflake.to.gcs.sf.password="pwd1234" \
--snowflake.to.gcs.sf.database="SNOWFLAKE_SAMPLE_DATA" \
--snowflake.to.gcs.sf.schema="TPCDS_SF100TCL" \
--snowflake.to.gcs.sf.query="SELECT * FROM CALL_CENTER" \
--snowflake.to.gcs.sf.autopushdown="off" \
--snowflake.to.gcs.output.location="gs://test-bucket/snowflake" \
--snowflake.to.gcs.output.format="avro" \
--snowflake.to.gcs.partition.column="CC_CALL_CENTER_SK" 
```