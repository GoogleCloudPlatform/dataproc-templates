# JDBC to JDBC Data Migration on Dataproc Serverless

This document provides instructions on how to run the JDBC to JDBC data migration Spark job on Dataproc Serverless.

## 1. Package the Jar

First, package the application JAR file using Maven:

```bash
mvn clean package
```

## 2. Set up Google Cloud Storage

Create a bucket and upload the packaged JAR file to it:

```bash
gsutil mb gs://<your-bucket-name>
gsutil cp target/gemini-java-templates-1.0-SNAPSHOT.jar gs://<your-bucket-name>/
```

## 3. Set up Secret Manager

Store your JDBC connection details in Google Secret Manager. You will need to create secrets for:

*   Postgres URL (should include username and password, e.g., `jdbc:postgresql://host:port/database?user=myuser&password=mypassword`)
*   MySQL URL (should include username and password, e.g., `jdbc:mysql://host:port/database?user=myuser&password=mypassword`)

## 4. Run the Job on Dataproc Serverless

Submit the Spark job to Dataproc Serverless using the `gcloud` command-line tool:

```bash
gcloud dataproc batches submit spark \
    --project=<your-gcp-project-id> \
    --region=<your-gcp-region> \
    --class=com.google.cloud.dataproc.templates.jdbc.JdbcToJdbc \
    --jars=gs://<your-bucket-name>/gemini-java-templates-1.0-SNAPSHOT.jar \
    -- \
    jdbc.input.url.secret.id=<postgres-url-secret-id> \
    jdbc.input.table=<postgres-table-name> \
    jdbc.input.partition.column=<partition-column-name> \
    jdbc.input.lower.bound=<lower-bound> \
    jdbc.input.upper.bound=<upper-bound> \
    jdbc.input.num.partitions=<num-partitions> \
    jdbc.output.url.secret.id=<mysql-url-secret-id> \
    jdbc.output.table=<mysql-table-name> \
    jdbc.batch.size=1000
```

Replace the placeholders `<...>` with your actual values.