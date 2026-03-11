# Migrating Data from MongoDB to BigQuery

This document provides instructions on how to run a Spark job in Java to migrate a collection from MongoDB to BigQuery using Dataproc Serverless.

## Prerequisites

1.  **Google Cloud Project:** A project with the Dataproc and BigQuery APIs enabled.
2.  **GCS Bucket:** A bucket to store the compiled JAR and for BigQuery's temporary data.
3.  **MongoDB Instance:** A reachable MongoDB instance (e.g., MongoDB Atlas or on GCE).
4.  **BigQuery Dataset:** A dataset where the table will be created.

## Building the JAR

Build the project using Maven to create the shaded JAR:

```bash
mvn clean package
```

The output JAR will be `target/spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar`.

## Running on Dataproc Serverless

Use the `gcloud dataproc batches submit spark` command to run the job.

```bash
gcloud dataproc batches submit spark \
    --project=<PROJECT_ID> \
    --region=<REGION> \
    --batch=mongo-to-bq-job \
    --class=com.customer.app.MongoToBigQuery \
    --jars=target/spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar \
    -- \
    "<MONGO_URI>" \
    "<MONGO_DATABASE>" \
    "<MONGO_COLLECTION>" \
    "<BQ_DATASET.TABLE>" \
    "<TEMP_GCS_BUCKET>"
```

### Parameters:

*   `<PROJECT_ID>`: Your Google Cloud project ID.
*   `<REGION>`: The region where you want to run the batch (e.g., `us-central1`).
*   `<MONGO_URI>`: The connection string for your MongoDB instance.
*   `<MONGO_DATABASE>`: The name of the MongoDB database.
*   `<MONGO_COLLECTION>`: The name of the MongoDB collection to migrate.
*   `<BQ_DATASET.TABLE>`: The destination BigQuery table in the format `dataset.table`.
*   `<TEMP_GCS_BUCKET>`: A GCS bucket used by the BigQuery connector for temporary data.

## Note on Networking

Ensure that the Dataproc Serverless environment has network access to your MongoDB instance. If MongoDB is outside of Google Cloud, you might need to configure Cloud NAT or a VPN. If it's on MongoDB Atlas, ensure the IP addresses of your Dataproc workers are whitelisted or use VPC Peering/Private Service Connect.
