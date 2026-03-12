# Session Summary: Cassandra to BigQuery Migration Job (Java)

In this session, we implemented a Java-based Spark job to migrate data from a Cassandra table to BigQuery.

## Key Changes:
- **Updated `pom.xml`**: Added the required Spark connectors for Cassandra and BigQuery:
  - `com.datastax.spark:spark-cassandra-connector_2.13`
  - `com.google.cloud.spark:spark-bigquery-with-dependencies_2.13`
- **New Java Application**: Created `com.customer.app.CassandraToBigQuery.java`. This Spark job:
  - Reads data from a specified Cassandra cluster, keyspace, and table.
  - Adds an `insertion_time` column using the project's utility method.
  - Writes the data to a BigQuery table using a temporary GCS bucket for data staging.
- **New Documentation**: Created `migrateCassandraToBigquery.md` with detailed instructions on building and submitting the job to Dataproc Serverless.

## How to use:
1.  Review and adjust parameters in the `gcloud` command provided in `migrateCassandraToBigquery.md`.
2.  Build the project with `mvn clean package`.
3.  Upload the fat JAR to your GCS bucket.
4.  Submit the job using `gcloud dataproc batches submit spark`.
