/*
 * Copyright (C) 2023 Google LLC
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
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToPubSub implements BaseTemplate {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToPubSub.class);
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private String pubsubCheckpointLocation;
  private String kafkaStartingOffsets;
  private Long kafkaAwaitTerminationTimeout;
  private String pubsubProject;
  private String pubsubTopic;

  public KafkaToPubSub() {
    kafkaBootstrapServers = getProperties().getProperty(KAFKA_PUBSUB_BOOTSTRAP_SERVERS);
    kafkaTopic = getProperties().getProperty(KAFKA_PUBSUB_INPUT_TOPIC);
    kafkaStartingOffsets = getProperties().getProperty(KAFKA_PUBSUB_STARTING_OFFSET);
    kafkaAwaitTerminationTimeout =
        Long.valueOf(getProperties().getProperty(KAFKA_PUBSUB_AWAIT_TERMINATION_TIMEOUT));
    pubsubProject = getProperties().getProperty(KAFKA_PUBSUB_OUTPUT_PROJECT_ID);
    pubsubTopic = getProperties().getProperty(KAFKA_PUBSUB_OUTPUT_TOPIC);
    pubsubCheckpointLocation = getProperties().getProperty(KAFKA_PUBSUB_CHECKPOINT_LOCATION);
  }

  @Override
  public void runTemplate() throws StreamingQueryException, TimeoutException {

    // Create a Spark session
    SparkSession spark = SparkSession.builder().appName("Spark KafkaToPubSub Job").getOrCreate();

    // Read data from Kafka
    Dataset<Row> df =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("subscribe", kafkaTopic)
            .option("startingOffsets", kafkaStartingOffsets)
            .load();

    // Extract the value and cast it to a double
    df = df.selectExpr("cast(value as String) value");

    // Send the data to Pub/Sub
    PubSubSink writer = new PubSubSink(pubsubTopic, pubsubProject);

    df.writeStream()
        .option("checkpointLocation", pubsubCheckpointLocation)
        .foreach(writer)
        .start()
        .awaitTermination(kafkaAwaitTerminationTimeout);
  }

  @Override
  public void validateInput() {
    if (StringUtils.isAllBlank(pubsubCheckpointLocation)
        || StringUtils.isAllBlank(kafkaBootstrapServers)
        || StringUtils.isAllBlank(kafkaTopic)
        || StringUtils.isAllBlank(pubsubProject)
        || StringUtils.isAllBlank(pubsubTopic)) {
      LOGGER.error(
          "{},{},{},{},{} are required parameters. ",
          KAFKA_PUBSUB_BOOTSTRAP_SERVERS,
          KAFKA_PUBSUB_INPUT_TOPIC,
          KAFKA_PUBSUB_OUTPUT_TOPIC,
          KAFKA_PUBSUB_OUTPUT_PROJECT_ID,
          KAFKA_PUBSUB_CHECKPOINT_LOCATION);
      throw new IllegalArgumentException(
          "Required parameters for KafkaToPubSub not passed. "
              + "Set mandatory parameter for KafkaToPubSub template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting Kafka to PubSub spark job with following parameters:\n"
            + "1. {}:{}\n"
            + "2. {}:{}\n"
            + "3. {}:{}\n"
            + "4. {}:{}\n"
            + "5. {}:{}\n"
            + "6. {}:{}\n"
            + "7. {}:{}\n",
        KAFKA_PUBSUB_BOOTSTRAP_SERVERS,
        kafkaBootstrapServers,
        KAFKA_PUBSUB_INPUT_TOPIC,
        kafkaTopic,
        KAFKA_PUBSUB_OUTPUT_TOPIC,
        pubsubTopic,
        KAFKA_PUBSUB_OUTPUT_PROJECT_ID,
        pubsubProject,
        KAFKA_PUBSUB_CHECKPOINT_LOCATION,
        pubsubCheckpointLocation,
        KAFKA_PUBSUB_STARTING_OFFSET,
        kafkaStartingOffsets,
        KAFKA_PUBSUB_AWAIT_TERMINATION_TIMEOUT,
        kafkaAwaitTerminationTimeout);
  }
}

class PubSubSink extends ForeachWriter<Row> {
  private static final long serialVersionUID = 1L;
  private Publisher publisher;
  private String project;
  private String topic;

  PubSubSink(String topic, String project) {
    this.topic = topic;
    this.project = project;
  }

  @Override
  public boolean open(long partitionId, long epochId) {
    // Creating a publisher for the topic
    ProjectTopicName topic = ProjectTopicName.of(this.project, this.topic);
    try {
      publisher = Publisher.newBuilder(topic).build();
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public void process(Row row) {
    // Converting the row to a Pub/Sub message and publishing to Pub/Sub topic
    ByteString message = ByteString.copyFromUtf8(row.getAs("value"));
    PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(message).build();
    publisher.publish(pubsubMessage);
  }

  @Override
  public void close(Throwable errorOrNull) {
    // Closing the connection
    publisher.shutdown();
  }
}
