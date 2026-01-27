# Migrating Data from Delta Lake to Iceberg

This document provides instructions on how to run a Spark job to migrate data from a Delta Lake table in Google Cloud Storage (GCS) to an Iceberg table.

## Prerequisites

*   A Dataproc Serverless cluster.
*   A GCS bucket for the Iceberg warehouse.
*   The JAR file for the Spark job.

## Building the JAR

To build the JAR file, run the following Maven command in your project's root directory:

```bash
mvn clean package
```

This will create a JAR file in the `target` directory named `spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar`.

## Running the Spark Job

Use the following `gcloud` command to submit the Spark job to Dataproc Serverless:

```bash
gcloud dataproc batches submit spark \
    --region=<region> \
    --project=<project-id> \
    --batch=delta-to-iceberg-migration \
    --class=com.customer.app.DeltaLakeToIceberg \
    --jars=target/spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar \
    -- \
    <delta-table-path> \
    <iceberg-table-name> \
    <timestamp>
```

### Arguments

*   `<region>`: The region for the Dataproc Serverless cluster.
*   `<project-id>`: Your Google Cloud project ID.
*   `<delta-table-path>`: The GCS path to the Delta Lake table (e.g., `gs://<bucket>/delta-table`).
*   `<iceberg-table-name>`: The name of the Iceberg table (e.g., `my_iceberg_table`).
*   `<timestamp>`: The timestamp for time travel in the format `yyyy-MM-dd HH:mm:ss`.

### Spark Configuration

You will also need to provide additional configuration to your Spark job for Iceberg to work correctly. Create a file named `spark-properties.conf` with the following content:

```
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type=hive
spark.sql.catalog.spark_catalog.warehouse=gs://<your-gcs-bucket>/iceberg-warehouse
```

Then, use the `--properties-file` flag when submitting your job:

```bash
gcloud dataproc batches submit spark \
    --region=<region> \
    --project=<project-id> \
    --batch=delta-to-iceberg-migration \
    --class=com.customer.app.DeltaLakeToIceberg \
    --jars=target/spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar \
    --properties-file=spark-properties.conf \
    -- \
    <delta-table-path> \
    <iceberg-table-name> \
    <timestamp>
```

Replace `<your-gcs-bucket>` with the name of your GCS bucket.

```
