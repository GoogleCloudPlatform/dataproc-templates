## 1. JDBC To BigQuery

Note - Add dependency jar's specific to database in jars variable. 

Example: export JARS=gs://<bucket_name>/mysql-connector-java.jar

General Execution:

```
GCP_PROJECT=<gcp-project-id> \
REGION=<region>  \
SUBNET=<subnet>   \
GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
HISTORY_SERVER_CLUSTER=<history-server> \
export JARS=<gcs_path_to_jar_files> \

bin/start.sh \
-- --template JDBCTOBIGQUERY \
--templateProperty jdbctobq.bigquery.location=<bigquery destination> \
--templateProperty jdbctobq.jdbc.url=<jdbc url> \
--templateProperty jdbctobq.jdbc.driver.class.name=<jdbc driver class name> \
--templateProperty jdbctobq.jdbc.properties=<jdbc properties in json format> \
--templateProperty jdbctobq.input.table=<source table> \
--templateProperty jdbctobq.input.db=<source database> \
--templateProperty jdbctobq.write.mode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty jdbctobq.spark.sql.warehouse.dir=<gcs path> \
```

### Configurable Parameters
Update Following properties in  [template.properties](../../../../../../../resources/template.properties) file:
```
# JDBCToBQ Template properties.
##Bigquery table name
jdbctobq.bigquery.location=
jdbctobq.jdbc.url=jdbc:mysql://host:port/
jdbctobq.jdbc.driver.class.name=com.mysql.jdbc.Driver
#Customize any properties supported by jdbc. Value to be in JSON format
jdbctobq.jdbc.properties={"user":"usernamehere","password":"passwordhere"}
##jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=nsadineni;OAuthType=1;
jdbctobq.input.table=
jdbctobq.input.db=
#Write mode to use while writing output to BQ. Supported values are - Append/Overwrite/ErrorIfExists/Ignore
jdbctobq.write.mode=
# Spark warehouse directory location
jdbctobq.spark.sql.warehouse.dir=
```
