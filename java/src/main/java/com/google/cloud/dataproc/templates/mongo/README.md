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
- `mongo.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)

## Example submission

```
export GCP_PROJECT=my-project
export JARS="gs://spark-lib/mongodb/mongo-spark-connector_2.12-2.4.0.jar,gs://spark-lib/mongodb/mongo-java-driver-3.9.1.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
export GCS_STAGING_LOCATION="gs://my-bucket"
export REGION=us-central1
export SUBNET=projects/my-project/regions/us-central1/subnetworks/test-subnet

./bin/start.sh \
-- \
--template MongoToBQ \
--templateProperty project.id=$GCP_PROJECT \
--templateProperty mongo.bq.input.uri="$ENV_TEST_MONGO_DB_URI" \
--templateProperty mongo.bq.input.database=demo \
--templateProperty mongo.bq.input.collection=dummyusers \
--templateProperty mongo.bq.output.dataset=dataproc_templates \
--templateProperty mongo.bq.output.table=mongotobq \
--templateProperty mongo.bq.output.mode=Append \
--templateProperty mongo.bq.temp.bucket.name=dataproc-templates/integration-testing/mongotobq
```
