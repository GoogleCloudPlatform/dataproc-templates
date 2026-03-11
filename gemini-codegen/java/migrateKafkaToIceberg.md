# Kafka to Iceberg Migration on Serverless Spark

This guide provides instructions on how to load data from a CSV file into a Kafka topic and then migrate that data into a GCS-based Iceberg table using Dataproc Serverless for Spark.

## Prerequisites

1.  **Kafka Cluster**: A running Kafka cluster accessible from where you run the `CsvToKafka` program and from Dataproc Serverless.
2.  **Google Cloud Storage (GCS)**: A bucket for Iceberg data and Spark checkpoints.
3.  **Dataproc Serverless**: Enabled in your Google Cloud project.
4.  **Iceberg Catalog**: Configured to use Hive or another compatible catalog (e.g., Hadoop).

## 1. Load CSV to Kafka

The `CsvToKafka` program reads a CSV file and sends each record as a JSON message to a Kafka topic.

### Build the Jar
```bash
mvn clean package
```

### Run CsvToKafka
```bash
java -cp target/spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar com.customer.app.CsvToKafka \
  <csv-file-path> \
  <kafka-bootstrap-servers> \
  <kafka-topic>
```

## 2. Migrate Kafka to Iceberg (Serverless Spark)

The `KafkaToIceberg` Spark job reads from Kafka using Structured Streaming with `Trigger.AvailableNow()` to perform a batch migration.

### Submit the Job to Dataproc Serverless

Replace the placeholders with your actual values.

```bash
gcloud dataproc batches submit pyspark \
  --project <project-id> \
  --region <region> \
  --batch <batch-id> \
  --container-image <optional-custom-image> \
  --jars gs://<your-bucket>/spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar \
  --class com.customer.app.KafkaToIceberg \
  -- \
  <kafka-bootstrap-servers> \
  <kafka-topic> \
  <iceberg-table-name> \
  gs://<your-bucket>/checkpoints/kafka-to-iceberg \
  '<json-schema>'
```

**Note:** Since this is a Java job, use `gcloud dataproc batches submit spark` if your primary class is Java.

```bash
gcloud dataproc batches submit spark \
  --project <project-id> \
  --region <region> \
  --batch <batch-id> \
  --class com.customer.app.KafkaToIceberg \
  --jars gs://<your-bucket>/spark-delta-to-iceberg-migration-1.0-SNAPSHOT.jar \
  -- \
  <kafka-bootstrap-servers> \
  <kafka-topic> \
  <iceberg-table-name> \
  gs://<your-bucket>/checkpoints/kafka-to-iceberg \
  '{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}'
```

### Important Configurations
The Spark session is configured to use Iceberg with a Hive catalog. You may need to adjust these configurations based on your specific Iceberg setup (e.g., if using a different catalog type or location).

```java
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
.config("spark.sql.catalog.spark_catalog.type", "hive")
```
