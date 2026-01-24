# Migrating Data from Postgres to MySQL using a Serverless Spark Job

This document provides instructions on how to run a Spark job to migrate data from a Postgres database to a MySQL database using a serverless Spark environment like Google Cloud Dataproc Serverless.
---
The text between the line above and line below was written by a human. The rest of the document was created by Gemini. The initial prompt to Gemini was:
```
Create a Spark job in Java to to migrate data from a table in a Postgres database to a table in MySQL, both accessible via JDBC; The JDBC URL strings are stored in Google secret manager. The URL string includes the username and password. Read and write data in parallel based on partitioning information that is provided; While writing, write data in batches for efficiency. Use the addInsertionTimeColumn to add a column to the data before writing it to MySQL destination table. Provide instructions to run this job on serverless spark in migrateJdbcToJdbc.md Provide a summary of the session in migrationREADME.md
```
Gemini generated the Java app, specifically the file `PostgresToMySql.java` and the README file. Changes were required to the generated code, including a)specifying drivers for Postgres and MySQL, b)adjusting partition bounds and number of partitions, converting a string argument to integer. At the time of this writing the generated `pom.xml` needed to be updated manually. Serverless for Apache Spark does not require a VPC subnet and the jdbc drivers are in the shaded Jar. The working gcloud command is:
```
gcloud dataproc batches submit spark --class=com.customer.app.PostgresToMySql \
    --jars=<bucket-location>/postgres-to-mysql-migration-1.0-SNAPSHOT.jar -- <postgres-table> <mysql-table> \
    <postgres-secret> <mysql-secret> <column> <batchsize>
```
---
## Prerequisites

1.  **Google Cloud SDK:** Make sure you have the Google Cloud SDK installed and configured on your local machine.
2.  **GCP Project:** You need a Google Cloud project with the Dataproc API enabled.
3.  **VPC Subnet:** A VPC subnet with Private Google Access enabled is required for the Dataproc Serverless job to access Google Cloud services.
4.  **JDBC Drivers:** The JDBC driver JAR files for Postgres and MySQL must be accessible in a Google Cloud Storage bucket.
5.  **Secrets:** The JDBC URL strings for both Postgres and MySQL must be stored in Google Secret Manager. The URL should include the username and password.

## Running the Job

1.  **Build the JAR:**
    Package the application into a fat JAR file using Maven:
    ```bash
    mvn clean package
    ```

2.  **Upload the JAR to GCS:**
    Upload the generated JAR file (`target/postgres-to-mysql-migration-1.0-SNAPSHOT.jar`) to a Google Cloud Storage bucket.

3.  **Submit the Dataproc Serverless Job:**
    Use the `gcloud` command to submit the Spark job to Dataproc Serverless. Replace the placeholders with your specific values.

    ```bash
    gcloud dataproc batches submit spark \
        --project=<your-gcp-project-id> \
        --region=<your-gcp-region> \
        --batch=postgres-to-mysql-migration \
        --class=com.customer.app.PostgresToMySql \
        --jars=gs://<your-gcs-bucket>/postgres-to-mysql-migration-1.0-SNAPSHOT.jar,gs://<your-gcs-bucket>/postgresql-42.3.3.jar,gs://<your-gcs-bucket>/mysql-connector-java-8.0.28.jar \
        --subnet=<your-vpc-subnet> \
        -- \
        <postgres.table> \
        <mysql.table> \
        <postgres.secret.id> \
        <mysql.secret.id> \
        <partition.column> \
        <batch.size>
    ```

### Arguments:
*   `<your-gcp-project-id>`: Your Google Cloud project ID.
*   `<your-gcp-region>`: The GCP region for the Dataproc job (e.g., `us-central1`).
*   `gs://<your-gcs-bucket>/...`: The GCS paths to the application JAR and the JDBC driver JARs.
*   `<your-vpc-subnet>`: The name of the VPC subnet to use.
*   `<postgres.table>`: The name of the source table in Postgres.
*   `<mysql.table>`: The name of the destination table in MySQL.
*   `<postgres.secret.id>`: The ID of the secret in Secret Manager containing the Postgres JDBC URL.
*   `<mysql.secret.id>`: The ID of the secret in Secret Manager containing the MySQL JDBC URL.
*   `<partition.column>`: The name of the column to use for partitioning the read from Postgres.
*   `<batch.size>`: The number of records to write in each batch to MySQL.

**Note:** In the `PostgresToMySql.java` file, you need to replace "your-gcp-project-id" in the `getSecret` method with your actual GCP project ID.
