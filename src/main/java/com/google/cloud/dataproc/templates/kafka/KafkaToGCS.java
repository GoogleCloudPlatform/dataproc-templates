/*
 * Copyright (C) 2021 Google LLC
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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark job to move data or/and schema from Kafka topic to GCS. This template can be configured to
 * run in few different modes. In default mode kafka.gcs.output.mode is set to "append". This will
 * write only the new row in streaming DataFrame/Dataset to write to sink. Other possible values for
 * this property are: 1. Complete: All the rows in the streaming DataFrame/Dataset will be written
 * to the sink every time there are some updates. 2. Update: Only the rows that were updated in the
 * streaming DataFrame/Dataset will be written to the sink every time there are some updates. For
 * detailed list of properties refer "KafkaToGCS Template properties" section in
 * resources/template.properties file.
 */
public class KafkaToGCS implements BaseTemplate {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToGCS.class);
  private String gcsOutputLocation;
  private String gcsOutputFormat;
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private String gcsCheckpointLocation;
  private String kafkaStartingOffsets;
  private String kafkaOutputMode;
  private Long kafkaAwaitTerminationTimeout;

  public KafkaToGCS() {
    gcsOutputLocation = getProperties().getProperty(KAFKA_GCS_OUTPUT_LOCATION);
    gcsOutputFormat = getProperties().getProperty(KAFKA_GCS_OUTPUT_FORMAT);
    kafkaBootstrapServers = getProperties().getProperty(KAFKA_GCS_BOOTSTRAP_SERVERS);
    kafkaTopic = getProperties().getProperty(KAFKA_GCS_TOPIC);
    gcsCheckpointLocation = gcsOutputLocation.concat("/checkpoint/");
    kafkaStartingOffsets = getProperties().getProperty(KAFKA_GCS_STARTING_OFFSET);
    kafkaOutputMode = getProperties().getProperty(KAFKA_GCS_OUTPUT_MODE);
    kafkaAwaitTerminationTimeout =
        Long.valueOf(getProperties().getProperty(KAFKA_GCS_AWAIT_TERMINATION_TIMEOUT));
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(gcsOutputLocation)
        || StringUtils.isAllBlank(kafkaBootstrapServers)
        || StringUtils.isAllBlank(kafkaTopic)) {
      LOGGER.error(
          "{},{},{} is required parameter. ",
          KAFKA_GCS_OUTPUT_LOCATION,
          KAFKA_GCS_BOOTSTRAP_SERVERS,
          KAFKA_GCS_TOPIC);
      throw new IllegalArgumentException(
          "Required parameters for KafkaToGCS not passed. "
              + "Set mandatory parameter for KafkaToGCS template "
              + "in resources/conf/template.properties file.");
    }

    SparkSession spark = null;
    LOGGER.info(
        "Starting Kafka to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5. {},{}"
            + "6. {},{}"
            + "7, {},{}",
        KAFKA_GCS_OUTPUT_LOCATION,
        gcsOutputLocation,
        KAFKA_GCS_OUTPUT_FORMAT,
        gcsOutputFormat,
        KAFKA_GCS_BOOTSTRAP_SERVERS,
        kafkaBootstrapServers,
        KAFKA_GCS_TOPIC,
        kafkaTopic,
        KAFKA_GCS_STARTING_OFFSET,
        kafkaStartingOffsets,
        KAFKA_GCS_OUTPUT_MODE,
        kafkaOutputMode,
        KAFKA_GCS_AWAIT_TERMINATION_TIMEOUT,
        kafkaAwaitTerminationTimeout);

    try {
      // Initialize Spark session
      spark = SparkSession.builder().appName("Spark KafkaToGCS Job").getOrCreate();

      LOGGER.debug("added jars : {}", spark.sparkContext().addedJars().keys());

      /** Read stream data from Kafka topic */
      Dataset<Row> inputData =
          spark
              .readStream()
              .format("kafka")
              .option("kafka.bootstrap.servers", kafkaBootstrapServers)
              .option("subscribe", kafkaTopic)
              .option("startingOffsets", kafkaStartingOffsets)
              .option("failOnDataLoss", "false")
              .load();

      Dataset<Row> processedData;

      // Convert the key and value from kafka message to String
      processedData =
          inputData
              .withColumn("key", inputData.col("key").cast(DataTypes.StringType))
              .withColumn("value", inputData.col("value").cast(DataTypes.StringType));

      // Write the output to GCS location
      processedData
          .writeStream()
          .format(gcsOutputFormat)
          .outputMode(kafkaOutputMode)
          .option("checkpointLocation", gcsCheckpointLocation)
          .option("path", gcsOutputLocation)
          .start()
          .awaitTermination(kafkaAwaitTerminationTimeout);

      LOGGER.info("KakfaToGCS job completed.");
      spark.stop();
    } catch (Throwable th) {
      LOGGER.error("Exception in KakfaToGCS", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
