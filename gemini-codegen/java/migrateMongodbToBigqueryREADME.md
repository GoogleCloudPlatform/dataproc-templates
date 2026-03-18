# Session Summary: MongoDB to BigQuery Migration

This session focused on implementing a Spark job in Java to migrate a collection from MongoDB to BigQuery on Dataproc Serverless.

## Key Changes

1.  **Dependencies:** Added the following dependencies to `pom.xml`:
    *   `org.mongodb.spark:mongo-spark-connector_2.13`
    *   `com.google.cloud.spark:spark-bigquery-with-dependencies_2.13`
2.  **Implementation:** Created `MongoToBigQuery.java` in `com.customer.app`. This job:
    *   Reads from a specified MongoDB URI, database, and collection.
    *   Uses `DataframeUtils` to add an `insertion_time` timestamp to the data.
    *   Writes the transformed data to a BigQuery table.
3.  **Instructions:** Provided detailed instructions in `migrateMongodbToBigquery.md` on:
    *   Prerequisites for the migration.
    *   Building the shaded JAR using Maven.
    *   Submitting the job as a Dataproc Serverless batch using `gcloud`.

## Running the Migration

To execute the migration, build the project and use the `gcloud` command provided in `migrateMongodbToBigquery.md`. The command requires the MongoDB URI, database name, collection name, destination BigQuery table, and a temporary GCS bucket for the BigQuery connector.
