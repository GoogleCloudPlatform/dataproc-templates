# Migrating Data from Cassandra to BigQuery using a Serverless Spark Job

This document provides instructions on how to run a Spark job to migrate data from a Cassandra table to a BigQuery table using Google Cloud Dataproc Serverless.
---
The text between the line above and line below was written by a human. The rest of the document was created by Gemini. The initial prompt to Gemini was:
```
Create Spark job in Java to to migrate a table from Cassandra to BigQuery. Provide instructions to run this job on serverless spark in migrateCassandraToBigquery.md and provide a summary of the session in migrateCassandraToBigqueryREADME.md 

```
Gemini generated the Java app, specifically the file `CassandraToBigquery.java` and the README file. Minor changes were required to accomodate that Bigquery .jar is included in Serverless Spark and jnr-posix is required by Cassandra. The working gcloud command is:
```
gcloud dataproc batches submit spark --project dataproc-templates --region us-central1 \
    --batch="cassandra-to-bigquery-$(date +%s)" --class com.customer.app.CassandraToBigQuery --version=2.2 \
    --jars=<REQUIRED_JARS> -- <CASSANDRA_HOST> <CASSANDRA_KEYSPACE> <CASSANDRA_KEYSPACE> \
    <DATASET.TABLE> <TEMP_BUCKET> <WRITE_MODE>
```
---
## Prerequisites

1.  **Google Cloud SDK:** Ensure you have the Google Cloud SDK installed and configured.
2.  **GCP Project:** A Google Cloud project with Dataproc and BigQuery APIs enabled.
3.  **VPC Subnet:** A VPC subnet with Private Google Access enabled for Dataproc Serverless.
4.  **Cassandra Accessibility:** The Cassandra cluster must be accessible from the Dataproc Serverless environment (e.g., via Cloud VPN or Interconnect if it's on-prem).
5.  **GCS Bucket:** A temporary GCS bucket for the BigQuery connector to store intermediate data.

## Running the Job

1.  **Build the Fat JAR:**
    Package the application and its dependencies:
    ```bash
    mvn clean package
    ```
    This will create `target/spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar`.

2.  **Upload the JAR to GCS:**
    Upload the generated JAR file to a Google Cloud Storage bucket.
    ```bash
    gsutil cp target/spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar gs://<your-gcs-bucket>/
    ```

3.  **Submit the Dataproc Serverless Job:**
    Use the `gcloud` command to submit the Spark job.

    ```bash
    gcloud dataproc batches submit spark \
        --project=<your-gcp-project-id> \
        --region=<your-gcp-region> \
        --batch=cassandra-to-bq-migration \
        --class=com.customer.app.CassandraToBigQuery \
        --jars=gs://<your-gcs-bucket>/spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar \
        --subnet=<your-vpc-subnet> \
        -- \
        <cassandra.host> \
        <cassandra.keyspace> \
        <cassandra.table> \
        <bq.table> \
        <bq.temp.bucket> \
        <write.mode>
    ```

### Arguments:
*   `<cassandra.host>`: The IP address or hostname of the Cassandra cluster.
*   `<cassandra.keyspace>`: The Cassandra keyspace.
*   `<cassandra.table>`: The source table in Cassandra.
*   `<bq.table>`: The destination table in BigQuery (format: `project:dataset.table`).
*   `<bq.temp.bucket>`: A GCS bucket used for temporary storage during the BigQuery write process.
*   `<write.mode>`: Spark save mode (e.g., `Overwrite`, `Append`, `ErrorIfExists`, `Ignore`).
