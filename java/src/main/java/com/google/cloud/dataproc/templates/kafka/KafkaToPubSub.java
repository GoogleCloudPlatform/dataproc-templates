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
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.Serializable;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToPubSub implements BaseTemplate, Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToPubSub.class);
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private String pubsubCheckpointLocation;
  private String kafkaStartingOffsets;
  private Long kafkaAwaitTerminationTimeout;

  public KafkaToPubSub() {
    kafkaBootstrapServers = getProperties().getProperty(KAFKA_PUBSUB_BOOTSTRAP_SERVERS);
    kafkaTopic = getProperties().getProperty(KAFKA_PUBSUB_TOPIC);
    pubsubCheckpointLocation = getProperties().getProperty(KAFKA_PUBSUB_CHECKPOINT_LOCATION);
    kafkaStartingOffsets = getProperties().getProperty(KAFKA_PUBSUB_STARTING_OFFSET);
    kafkaAwaitTerminationTimeout =
        Long.valueOf(getProperties().getProperty(KAFKA_PUBSUB_AWAIT_TERMINATION_TIMEOUT));
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(pubsubCheckpointLocation)
        || StringUtils.isAllBlank(kafkaBootstrapServers)
        || StringUtils.isAllBlank(kafkaTopic)) {
      LOGGER.error(
          "{},{},{} is required parameter. ",
          KAFKA_PUBSUB_CHECKPOINT_LOCATION,
          KAFKA_PUBSUB_BOOTSTRAP_SERVERS,
          KAFKA_PUBSUB_TOPIC);
      throw new IllegalArgumentException(
          "Required parameters for KafkaToPubSub not passed. "
              + "Set mandatory parameter for KafkaToPubSub template "
              + "in resources/conf/template.properties file.");
    }

    SparkSession spark = null;
    LOGGER.info(
        "Starting Kafka to PubSub spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5, {},{}",
        KAFKA_PUBSUB_CHECKPOINT_LOCATION,
        pubsubCheckpointLocation,
        KAFKA_PUBSUB_BOOTSTRAP_SERVERS,
        kafkaBootstrapServers,
        KAFKA_PUBSUB_TOPIC,
        kafkaTopic,
        KAFKA_PUBSUB_STARTING_OFFSET,
        kafkaStartingOffsets,
        KAFKA_PUBSUB_AWAIT_TERMINATION_TIMEOUT,
        kafkaAwaitTerminationTimeout);

    try {
      // Initialize the Spark session
      spark = SparkSession.builder().appName("Spark KafkaToPubSub Job").getOrCreate();

      LOGGER.debug("added jars : {}", spark.sparkContext().addedJars().keys());

      // Stream data from Kafka topic
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

      processedData =
          inputData.withColumn("value", inputData.col("value").cast(DataTypes.StringType));

      processedData
          .select("value")
          .writeStream()
          .option("checkpointLocation", pubsubCheckpointLocation)
          .foreach(
              new ForeachWriter<Row>() {
                Publisher publisher = null;
                TopicName topicName = null;

                @Override
                public boolean open(long partitionId, long version) {
                  // Open connection
                  try {
                    topicName = TopicName.of("", "");
                    publisher = Publisher.newBuilder(topicName).build();
                    return true;
                  } catch (Exception e) {
                    // TODO: handle exception
                    return true;
                  }
                }

                @Override
                public void process(Row row) {
                  // Write string to connection
                  ByteString data = ByteString.copyFromUtf8(row.getString(0));
                  PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
                  publisher.publish(pubsubMessage);
                }

                @Override
                public void close(Throwable errorOrNull) {
                  // Close the connection
                  // publisher.shutdown();
                }
              })
          .start()
          .awaitTermination(kafkaAwaitTerminationTimeout);

      LOGGER.info("KakfaToPubSub job completed.");
      spark.stop();
    } catch (Throwable th) {
      LOGGER.error("Exception in KakfaToPubSub", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
