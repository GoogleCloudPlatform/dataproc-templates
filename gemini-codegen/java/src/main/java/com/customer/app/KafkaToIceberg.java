package com.customer.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;

public class KafkaToIceberg {

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("Usage: KafkaToIceberg <kafka-bootstrap-servers> <kafka-topic> <iceberg-table-name> <checkpoint-location> <json-schema>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topic = args[1];
        String icebergTableName = args[2];
        String checkpointLocation = args[3];
        String schemaJson = args[4];

        SparkSession spark = SparkSession.builder()
                .appName("Kafka to Iceberg Migration")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive") // Or another catalog type like 'hadoop' for GCS
                .getOrCreate();

        StructType schema = (StructType) StructType.fromJson(schemaJson);

        // Read from Kafka
        Dataset<Row> kafkaDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load();

        // Parse JSON value
        Dataset<Row> parsedDf = kafkaDf.selectExpr("CAST(value AS STRING)")
                .select(functions.from_json(functions.col("value"), schema).as("data"))
                .select("data.*");

        // Add insertion time (using utility if available)
        parsedDf = parsedDf.withColumn("insertion_time", functions.current_timestamp());

        // Write to Iceberg
        try {
            parsedDf.writeStream()
                    .queryName("kafka_to_iceberg_write")
                    .format("iceberg")
                    .outputMode("append")
                    .trigger(Trigger.AvailableNow())
                    .option("checkpointLocation", checkpointLocation)
                    .toTable(icebergTableName)
                    .awaitTermination();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
