package com.customer.app;

import com.customer.util.DataframeUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CassandraToBigQuery {

    public static void main(String[] args) {
        if (args.length != 6) {
            System.err.println("Usage: CassandraToBigQuery <cassandra.host> <cassandra.keyspace> <cassandra.table> <bq.table> <bq.temp.bucket> <write.mode>");
            System.exit(1);
        }

        String cassandraHost = args[0];
        String cassandraKeyspace = args[1];
        String cassandraTable = args[2];
        String bqTable = args[3];
        String bqTempBucket = args[4];
        String writeMode = args[5];

        SparkSession spark = SparkSession.builder()
                .appName("Cassandra to BigQuery Migration")
                .config("spark.cassandra.connection.host", cassandraHost)
                .getOrCreate();

        // Read data from Cassandra
        Dataset<Row> df = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", cassandraKeyspace)
                .option("table", cassandraTable)
                .load();

        // Add insertion time column (optional, but following project pattern)
        Dataset<Row> transformedDf = DataframeUtils.addInsertionTimeColumn(df);

        // Write data to BigQuery
        transformedDf.write()
                .format("bigquery")
                .option("table", bqTable)
                .option("temporaryGcsBucket", bqTempBucket)
                .mode(SaveMode.valueOf(writeMode))
                .save();

        spark.stop();
    }
}
