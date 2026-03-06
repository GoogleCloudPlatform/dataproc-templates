package com.customer.util;

import static org.apache.spark.sql.functions.from_json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

public class PubSubToSpanner {

    private static StructType getJsonSchema() {
        // TODO: This schema can be fetched from a schema registry,
        // a config file, or hardcoded as shown here.
        // This is an example schema.
        return new StructType()
            .add("id", "string")
            .add("name", "string")
            .add("value", "double");
    }

    public static void main(String[] args) throws Exception {
        // TODO: Pass these as arguments
        String projectId = "your-gcp-project-id";
        String subscriptionId = "your-pubsub-subscription";
        String spannerInstanceId = "your-spanner-instance-id";
        String spannerDatabaseId = "your-spanner-database-id";
        String spannerTable = "your-spanner-table";

        SparkSession spark = SparkSession.builder()
            .appName("PubSubToSpanner")
            .getOrCreate();

        // Read from Pub/Sub
        Dataset<Row> df = spark.readStream()
            .format("pubsub")
            .option("project.id", projectId)
            .option("subscription", subscriptionId)
            .load();

        // Get the schema for the JSON payload
        StructType jsonSchema = getJsonSchema();

        // Decode the message payload and parse JSON
        Dataset<Row> messages = df.select(
                from_json(df.col("data").cast("string"), jsonSchema).as("json")
            ).select("json.*");


        // Write to Spanner using the Spark Connector
        StreamingQuery query = messages.writeStream()
            .format("cloud-spanner") // User requested change
            .option("spanner.instanceId", spannerInstanceId)
            .option("spanner.databaseId", spannerDatabaseId)
            .option("spanner.table", spannerTable)
            .option("checkpointLocation", "/tmp/spark-checkpoint") // TODO: Use a GCS path for checkpointing
            .start();

        query.awaitTermination();
    }
}
