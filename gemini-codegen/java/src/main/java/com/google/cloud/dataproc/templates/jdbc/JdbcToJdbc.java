/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataproc.templates.jdbc;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

public class JdbcToJdbc {

    private static final String PROJECT_ID = "your-gcp-project-id"; // TODO: Make this a parameter
    private static final String JDBC_INPUT_URL_SECRET_ID = "jdbc.input.url.secret.id";
    private static final String JDBC_INPUT_TABLE = "jdbc.input.table";
    private static final String JDBC_PARTITION_COLUMN = "jdbc.input.partition.column";
    private static final String JDBC_LOWER_BOUND = "jdbc.input.lower.bound";
    private static final String JDBC_UPPER_BOUND = "jdbc.upper.bound";
    private static final String JDBC_NUM_PARTITIONS = "jdbc.num.partitions";
    private static final String JDBC_OUTPUT_URL_SECRET_ID = "jdbc.output.url.secret.id";
    private static final String JDBC_OUTPUT_TABLE = "jdbc.output.table";
    private static final String JDBC_BATCH_SIZE = "jdbc.batch.size";

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(new StringReader(String.join("\n", args)));

        SparkSession spark = SparkSession.builder()
                .appName("JDBC to JDBC Data Migration")
                .getOrCreate();

        // Read from Postgres
        Dataset<Row> sourceDf = spark.read()
                .format("jdbc")
                .option("url", getSecret(properties.getProperty(JDBC_INPUT_URL_SECRET_ID)))
                .option("dbtable", properties.getProperty(JDBC_INPUT_TABLE))
                .option("partitionColumn", properties.getProperty(JDBC_PARTITION_COLUMN))
                .option("lowerBound", properties.getProperty(JDBC_LOWER_BOUND))
                .option("upperBound", properties.getProperty(JDBC_UPPER_BOUND))
                .option("numPartitions", properties.getProperty(JDBC_NUM_PARTITIONS))
                .load();

        // Add insertion time
        Dataset<Row> transformedDf = sourceDf.withColumn("insertion_time", functions.current_timestamp());

        // Write to MySQL
        transformedDf.write()
                .format("jdbc")
                .option("url", getSecret(properties.getProperty(JDBC_OUTPUT_URL_SECRET_ID)))
                .option("dbtable", properties.getProperty(JDBC_OUTPUT_TABLE))
                .option("batchsize", properties.getProperty(JDBC_BATCH_SIZE))
                .mode(SaveMode.Append)
                .save();
    }

    /**
     * Retrieves a secret from Google Secret Manager.
     *
     * @param secretId The ID of the secret to retrieve.
     * @return The secret value.
     * @throws IOException If an I/O error occurs.
     */
    private static String getSecret(String secretId) throws IOException {
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            SecretVersionName secretVersionName = SecretVersionName.of(PROJECT_ID, secretId, "latest");
            AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
            return response.getPayload().getData().toStringUtf8();
        }
    }
}