# Migrating Data from MongoDB to BigQuery

This document provides instructions on how to run a Spark job in Java to migrate a collection from MongoDB to BigQuery using Dataproc Serverless.
---
The text between the line above and line below was written by a human. The rest of the document was created by Gemini. The initial prompt to Gemini was:
```
Create Spark job in Java to to migrate a collection from mongodb to BigQuery. Provide instructions to run this job on serverless spark in migrateMongodbToBigquery.md and provide a summary of the session in migrateMongodbToBigqueryREADME.md.

```
Gemini generated the Java app, specifically the file `MongoToBigQuery.java` and the README file. A small change was required to Gemini updated `pom.xml` specifying the BigQuery connector version and the scope. No changes were needed to the Java code. The working gcloud command is:
```
gcloud dataproc batches submit spark --project dataproc-templates --region us-central1 \
   --batch="mongodb-to-bigquery-$(date +%s)" --class com.customer.app.MongoToBigQuery --version=2.2 \
   --jars=<JARS> -- <MONGODB_URI> <DATABASE> <COLLECTION> <BQDATASET.TABLE> <TEMP_GCS_BUCKET>
```
---
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
