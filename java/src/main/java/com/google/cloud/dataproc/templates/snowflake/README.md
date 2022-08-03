## 1. Snowflake To GCS

General Execution:

```
export GCP_PROJECT=<gcp-project-id>
export REGION=<gcp-project-region>
export GCS_STAGING_LOCATION=<gcs-bucket-staging-folder-path>
export SUBNET=<gcp-project-dataproc-clusters-subnet>

bin/start.sh \
-- \
--template SNOWFLAKETOGCS \
--templateProperty snowflake.gcs.sfurl <snowflake-account-url> \
--templateProperty snowflake.gcs.sfuser <snowflake-user> \
--templateProperty snowflake.gcs.sfpassword <snowflake-user-password> \
--templateProperty snowflake.gcs.sfdatabase <snowflake-database> \
--templateProperty snowflake.gcs.sfschema <snowflake-schema> \
--templateProperty snowflake.gcs.sfwarehouse <snowflake-warehouse> \
--templateProperty snowflake.gcs.query <snowflake-select-query> \
--templateProperty snowflake.gcs.output.location <gcs-output-location> \
--templateProperty snowflake.gcs.output.format <csv|avro|orc|json|parquet> \
--templateProperty snowflake.gcs.output.mode <Overwrite|ErrorIfExists|Append|Ignore> \
--templateProperty snowflake.gcs.output.partitionColumn <gcs-output-partitionby-columnname> \
--templateProperty snowflake.gcs.autopushdown <on|off>
```

### Configurable Parameters
Following properties are avaialble in commandline or [template.properties](../../../../../../../resources/template.properties) file:

```
# Snowflake account URL. Format: <account-identifier>.snowflakecomputing.com
snowflake.gcs.sfurl=

# Snowflake username
snowflake.gcs.sfuser=

# Snowflake user password
snowflake.gcs.sfpassword=

# Snowflake database name
snowflake.gcs.sfdatabase=

# Snowflake schema name
snowflake.gcs.sfschema=

# Optional property: Snowflake warehouse
snowflake.gcs.sfwarehouse=

# Optional property: Snowflake query pushdown feature
snowflake.gcs.autopushdown=on
Note: If not specified explicitly through execution command, the default value is on.

# Snowflake input table
snowflake.gcs.table=

# Snowflake select query
snowflake.gcs.query=
Note: Either one of the template properties snowflake.gcs.table and snowflake.gcs.query must be provided.

# GCS output location. Format: gs://<bucket-name>/<dir>
snowflake.gcs.output.location=

# GCS ouput file format. Accepted values: csv, avro, orc, json or parquet
snowflake.gcs.output.format= 

# Optional property: GCS ouput write mode. Accepted values: Overwrite, ErrorIfExists, Append or Ignore
snowflake.gcs.output.mode=overwrite
Note: If not specified explicitly through execution command, the default value is Overwrite.

# Optional property: GCS output data partiton by column name
snowflake.gcs.output.partitionColumn=
```

If enabled, this feature leverages the performance efficiencies by enabling large and complex Spark logical plans (in their entirety or in parts) to be processed in Snowflake, thus using Snowflake to do most of the actual work. Accepted values: on, off
Note: If not specified explicitly through execution command, the default behaviour of pushdown is enabled with Spark-Snowflake connector.

### Important properties

* Usage of `snowflake.gcs.autopushdown`
    * If enabled, this feature leverages the performance efficiencies by enabling large and complex Spark logical plans (in their entirety or in parts) to be processed in Snowflake, thus using Snowflake to do most of the actual work.
        ```
        --templateProperty snowflake.gcs.autopushdown off
        ```
    Note: If not specified explicitly through execution command, the default behaviour of pushdown is enabled with Spark-Snowflake connector.

    To read more this property refer [Snowflake Docs: Overview of the Spark Connector](https://docs.snowflake.com/en/user-guide/spark-connector-overview.html#query-pushdown)

* Usage of `snowflake.gcs.sfwarehouse`
    * The Snowflake warehouse to use.
        ```
        --templateProperty  snowflake.gcs.sfwarehouse mywh
        ```
    Note: If not specified explicitly, it will take the default virtual warehouse configured at Snowflake.

* Usage of `snowflake.gcs.table` and `snowflake.gcs.query`
    * Provide the table name or an select equivalent query
        ```
        --templateProperty  snowflake.gcs.table CUSTOMER
        ```
        ```
        --templateProperty  snowflake.gcs.query 'SELECT DISTINCT(C_NATIONKEY) FROM CUSTOMER''
        ```
    Note: Only one of the above property should be provided through the execution command.


### Example submission
```
export GCP_PROJECT=my-gcp-project
export REGION=us-west1
export SUBNET=test-subnet
export GCS_STAGING_LOCATION=gs://templates-demo-sftogcs
bin/start.sh \
-- \
-- --template SNOWFLAKETOGCS \
--templateProperty snowflake.gcs.sfurl sdtr748374.snowflakecomputing.com \
--templateProperty snowflake.gcs.sfuser demo_user \
--templateProperty snowflake.gcs.sfpassword password_comes_here \
--templateProperty snowflake.gcs.sfdatabase SNOWFLAKE_SAMPLE_DATA \
--templateProperty snowflake.gcs.sfschema TPCH_SF1 \
--templateProperty snowflake.gcs.query 'SELECT * FROM CUSTOMER' \
--templateProperty snowflake.gcs.autopushdown off \
--templateProperty snowflake.gcs.output.location gs://templates-demo-sftogcs/out \
--templateProperty snowflake.gcs.output.format parquet \
--templateProperty snowflake.gcs.output.partitionColumn C_NATIONKEY
```