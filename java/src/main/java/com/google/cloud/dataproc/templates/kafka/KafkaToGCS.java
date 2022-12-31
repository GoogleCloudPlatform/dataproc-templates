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
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
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
  private String gcsCheckpointLocation;
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private String kafkaMessageFormat;
  private String kafkaSchemaUrl;
  private String kafkaStartingOffsets;
  private String kafkaOutputMode;
  private Long kafkaAwaitTerminationTimeout;
  private final String sparkLogLevel;

  public KafkaToGCS() {

    gcsOutputLocation = getProperties().getProperty(KAFKA_GCS_OUTPUT_LOCATION);
    gcsOutputFormat = getProperties().getProperty(KAFKA_GCS_OUTPUT_FORMAT);
    kafkaBootstrapServers = getProperties().getProperty(KAFKA_BOOTSTRAP_SERVERS);
    kafkaTopic = getProperties().getProperty(KAFKA_TOPIC);
    kafkaMessageFormat = getProperties().getProperty(KAFKA_MESSAGE_FORMAT);
    kafkaSchemaUrl = getProperties().getProperty(KAFKA_SCHEMA_URL);
    gcsCheckpointLocation = gcsOutputLocation.concat("/checkpoint/");
    kafkaStartingOffsets = getProperties().getProperty(KAFKA_STARTING_OFFSET);
    kafkaOutputMode = getProperties().getProperty(KAFKA_GCS_OUTPUT_MODE);
    kafkaAwaitTerminationTimeout =
        Long.valueOf(getProperties().getProperty(KAFKA_GCS_AWAIT_TERMINATION_TIMEOUT));
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() throws TimeoutException, StreamingQueryException {
    validateInput();

    // Initialize Spark session
    SparkSession spark = SparkSession.builder().appName("Spark KafkaToGCS Job").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    KafkaReader reader = new KafkaReader();

    LOGGER.info("Calling Kafka Reader");

    // Reading Kafka stream into dataframe
    Dataset<Row> processedData = reader.readKafkaTopic(spark, getProperties());

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
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(gcsOutputLocation)
        || StringUtils.isAllBlank(kafkaBootstrapServers)
        || StringUtils.isAllBlank(kafkaTopic)) {
      LOGGER.error(
          "{},{},{} is required parameter. ",
          KAFKA_GCS_OUTPUT_LOCATION,
          KAFKA_BOOTSTRAP_SERVERS,
          KAFKA_TOPIC);
      throw new IllegalArgumentException(
          "Required parameters for KafkaToGCS not passed. "
              + "Set mandatory parameter for KafkaToGCS template "
              + "in resources/conf/template.properties file.");
    }

    if (kafkaMessageFormat.equals("json") & StringUtils.isAllBlank(kafkaSchemaUrl)) {
      LOGGER.error("{} is a required parameter for JSON format messages", KAFKA_SCHEMA_URL);
      throw new IllegalArgumentException("Required parameters for KafkaToGCS not passed.");
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
        KAFKA_BOOTSTRAP_SERVERS,
        kafkaBootstrapServers,
        KAFKA_TOPIC,
        kafkaTopic,
        KAFKA_STARTING_OFFSET,
        kafkaStartingOffsets,
        KAFKA_GCS_OUTPUT_MODE,
        kafkaOutputMode,
        KAFKA_GCS_AWAIT_TERMINATION_TIMEOUT,
        kafkaAwaitTerminationTimeout);
  }
}
