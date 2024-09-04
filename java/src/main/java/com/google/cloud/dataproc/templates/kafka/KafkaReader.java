/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataproc.templates.kafka;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_BOOTSTRAP_SERVERS;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_MESSAGE_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_SCHEMA_URL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_STARTING_OFFSET;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_TOPIC;

import com.google.cloud.dataproc.templates.util.ReadSchemaUtil;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReader.class);

  private String kakfaMessageFormat;
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private String kafkaStartingOffsets;
  private String kafkaSchemaUrl;

  public Dataset<Row> readKafkaTopic(SparkSession spark, Properties prop) {

    kafkaBootstrapServers = prop.getProperty(KAFKA_BOOTSTRAP_SERVERS);
    kafkaTopic = prop.getProperty(KAFKA_TOPIC);
    kakfaMessageFormat = prop.getProperty(KAFKA_MESSAGE_FORMAT);
    kafkaStartingOffsets = prop.getProperty(KAFKA_STARTING_OFFSET);
    kafkaSchemaUrl = prop.getProperty(KAFKA_SCHEMA_URL);

    Dataset<Row> inputData =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("subscribe", kafkaTopic)
            .option("startingOffsets", kafkaStartingOffsets)
            .option("failOnDataLoss", "false")
            .load();

    // check on memory constraints

    return getDatasetByMessageFormat(inputData, prop);
  }

  public Dataset<Row> getDatasetByMessageFormat(Dataset<Row> inputData, Properties prop) {
    Dataset<Row> processedData = null;
    String kakfaMessageFormat = prop.getProperty(KAFKA_MESSAGE_FORMAT);
    String kafkaSchemaUrl = prop.getProperty(KAFKA_SCHEMA_URL);

    switch (kakfaMessageFormat) {
      case "bytes":
        processedData = processBytesMessage(inputData);
        break;

      case "json":
        StructType schema = ReadSchemaUtil.readSchema(kafkaSchemaUrl);
        processedData = processJsonMessage(inputData, schema);
        break;
    }

    return processedData;
  }

  public Dataset<Row> processJsonMessage(Dataset<Row> df, StructType schema) {

    Dataset<Row> processedDF =
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .select(
                functions.col("key"),
                functions.from_json(functions.col("value"), schema).alias("json_value"))
            .select("key", "json_value.*");

    return processedDF;
  }

  public Dataset<Row> processBytesMessage(Dataset<Row> df) {

    Dataset<Row> processedDF =
        df.withColumn("key", functions.col("key").cast(DataTypes.StringType))
            .withColumn("value", functions.col("value").cast(DataTypes.StringType));

    return processedDF;
  }
}
