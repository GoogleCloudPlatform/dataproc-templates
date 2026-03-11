package com.customer.app;

import com.customer.util.DataframeUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class MongoToBigQuery {

    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("Usage: MongoToBigQuery <mongo.uri> <mongo.database> <mongo.collection> <bq.dataset.table> <temp.gcs.bucket>");
            System.exit(1);
        }

        String mongoUri = args[0];
        String mongoDatabase = args[1];
        String mongoCollection = args[2];
        String bqDatasetTable = args[3];
        String tempGcsBucket = args[4];

        SparkSession spark = SparkSession.builder()
                .appName("MongoDB to BigQuery Migration")
                .getOrCreate();

        // Read data from MongoDB
        Dataset<Row> df = spark.read()
                .format("mongodb")
                .option("connection.uri", mongoUri)
                .option("database", mongoDatabase)
                .option("collection", mongoCollection)
                .load();

        // Add insertion time column
        Dataset<Row> transformedDf = DataframeUtils.addInsertionTimeColumn(df);

        // Write data to BigQuery
        transformedDf.write()
                .format("bigquery")
                .option("table", bqDatasetTable)
                .option("temporaryGcsBucket", tempGcsBucket)
                .mode(SaveMode.Overwrite)
                .save();

        spark.stop();
    }
}
