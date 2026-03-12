# Session Summary: CSV to Kafka to Iceberg Migration

## Overview
In this session, we developed a two-step data migration pipeline:
1.  **CSV to Kafka**: A standalone Java program (`CsvToKafka.java`) that reads a CSV file and produces JSON messages to a Kafka topic.
2.  **Kafka to Iceberg**: A Spark Structured Streaming job in Java (`KafkaToIceberg.java`) that reads data from a Kafka topic, parses the JSON, and writes it to an Iceberg table on Google Cloud Storage (GCS) using Dataproc Serverless.

## Key Components

### 1. `CsvToKafka.java`
- Uses `apache-kafka-clients` for message production.
- Uses `commons-csv` for robust CSV parsing.
- Converts CSV records into a simple JSON format.
- **Usage**: Local execution or via a VM with access to the Kafka cluster.

### 2. `KafkaToIceberg.java`
- Uses Spark Structured Streaming with `Trigger.AvailableNow()` for "batch-like" processing of streaming data.
- Dynamically parses JSON based on a provided schema string.
- Automatically adds an `insertion_time` column to each record.
- **Usage**: Scalable execution on Dataproc Serverless for Spark.

### 3. Documentation (`migrateKafkaToIceberg.md`)
- Detailed instructions for building the project, running the CSV loader, and submitting the Spark job to Google Cloud.

## Project Updates
- **`pom.xml`**: Added dependencies for `spark-sql-kafka-0-10_2.13`, `kafka-clients`, and `commons-csv`.
- **Source Code**: Created `com.customer.app.CsvToKafka` and `com.customer.app.KafkaToIceberg`.
- **Validation**: Verified the project compiles successfully using Maven.
