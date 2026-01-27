package com.customer.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DeltaLakeToIceberg {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: DeltaLakeToIceberg <delta-table-path> <iceberg-table-name> <timestamp>");
            System.exit(1);
        }

        String deltaTablePath = args[0];
        String icebergTableName = args[1];
        String timestamp = args[2];

        SparkSession spark = SparkSession.builder()
                .appName("Delta Lake to Iceberg Migration")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .getOrCreate();

        // Read data from Delta Lake table using time travel
        Dataset<Row> df = spark.read()
                .format("delta")
                .option("timestampAsOf", timestamp)
                .load(deltaTablePath);

        // Write data to Iceberg table
        df.write()
                .format("iceberg")
                .mode("overwrite")
                .save(icebergTableName);

        spark.stop();
    }
}
