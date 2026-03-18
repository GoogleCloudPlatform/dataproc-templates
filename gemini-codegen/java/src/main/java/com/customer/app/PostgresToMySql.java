package com.customer.app;

import com.customer.util.DataframeUtils;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class PostgresToMySql {

    public static void main(String[] args) {
        if (args.length != 6) {
            System.err.println("Usage: PostgresToMySql <postgres.table> <mysql.table> <postgres.secret.id> <mysql.secret.id> <partition.column> <batch.size>");
            System.exit(1);
        }

        String postgresTable = args[0];
        String mysqlTable = args[1];
        String postgresSecretId = args[2];
        String mysqlSecretId = args[3];
        String partitionColumn = args[4];
        String batchSize = args[5];

        SparkSession spark = SparkSession.builder()
                .appName("Postgres to MySQL JDBC Migration")
                .getOrCreate();

        String postgresUrl = getSecret(postgresSecretId);
        String mysqlUrl = getSecret(mysqlSecretId);

        // Read data from Postgres in parallel
        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", postgresUrl)
                .option("dbtable", postgresTable)
                .option("partitionColumn", partitionColumn)
                .option("lowerBound", "2012-01-03")
                .option("upperBound", "2025-10-31") // These bounds should be configured based on data
                .option("numPartitions", "1")
                .option("driver", "org.postgresql.Driver")
                .load();

        // Add insertion time column
        Dataset<Row> transformedDf = DataframeUtils.addInsertionTimeColumn(df);

        // Write data to MySQL in batches
        transformedDf.write()
                .format("jdbc")
                .option("url", mysqlUrl)
                .option("dbtable", mysqlTable)
                .option("batchsize", Integer.parseInt(batchSize))
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .mode(SaveMode.Overwrite)
                .save();

        spark.stop();
    }

    private static String getSecret(String secretId) {
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            SecretVersionName secretVersionName = SecretVersionName.of("dataproc-templates", secretId, "latest");
            return client.accessSecretVersion(secretVersionName).getPayload().getData().toStringUtf8();
        } catch (IOException e) {
            throw new RuntimeException("Failed to retrieve secret", e);
        }
    }
}
