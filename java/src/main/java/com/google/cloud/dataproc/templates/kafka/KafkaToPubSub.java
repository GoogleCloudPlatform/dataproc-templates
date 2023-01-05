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
import java.util.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToPubSub implements BaseTemplate {
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

    validateInput();

    JavaStreamingContext jsc = null;

    try {
      // Initialize the Spark session
      SparkConf sparkConf = new SparkConf().setAppName("Spark KafkaToPubSub Job");
      jsc = new JavaStreamingContext(sparkConf, Milliseconds.apply(kafkaAwaitTerminationTimeout));

      Collection<String> topics = Arrays.asList(kafkaTopic.split(","));
      Map<String, Object> kafkaParams = new HashMap<>();
      kafkaParams.put("bootstrap.servers", kafkaBootstrapServers);
      kafkaParams.put("key.deserializer", StringDeserializer.class);
      kafkaParams.put("value.deserializer", StringDeserializer.class);
      kafkaParams.put("group.id", "kafka_to_pubsub_dataproc_template");
      kafkaParams.put("auto.offset.reset", kafkaStartingOffsets);

      // Create direct kafka stream with brokers and topics
      JavaInputDStream<ConsumerRecord<String, String>> stream =
          KafkaUtils.createDirectStream(
              jsc,
              LocationStrategies.PreferConsistent(),
              ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

      writeToPubSub(stream, pubsubCheckpointLocation, kafkaAwaitTerminationTimeout);

      jsc.start();
      jsc.awaitTerminationOrTimeout(kafkaAwaitTerminationTimeout);

      LOGGER.info("KakfaToPubSub job completed.");

      jsc.stop();
    } catch (Throwable th) {
      LOGGER.error("Exception in KakfaToPubSub", th);
      if (Objects.nonNull(jsc)) {
        jsc.stop();
      }
    }
  }

  // foreachRDD
  // static void writeToPubSub(processedData,pubsubCheckpointLocation){
  // }

  // foreach
  static void writeToPubSub(
      JavaInputDStream<ConsumerRecord<String, String>> stream,
      String pubsubCheckpointLocation,
      Long kafkaAwaitTerminationTimeout)
      throws Exception {
    stream.foreachRDD(
        new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
          @Override
          public void call(JavaRDD<ConsumerRecord<String, String>> kafkaMessageJavaRDD)
              throws Exception {
            kafkaMessageJavaRDD.foreachPartition(
                new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
                  @Override
                  public void call(Iterator<ConsumerRecord<String, String>> kafkaMessageIterator)
                      throws Exception {

                    Publisher publisher = null;
                    TopicName topicName = null;
                    topicName = TopicName.of("yadavaja-sandbox", "kafkatopubsub");
                    publisher = Publisher.newBuilder(topicName).build();

                    while (kafkaMessageIterator.hasNext()) {
                      ConsumerRecord<String, String> message = kafkaMessageIterator.next();
                      ByteString data = ByteString.copyFromUtf8(message.value());
                      PubsubMessage pubsubMessage =
                          PubsubMessage.newBuilder().setData(data).build();
                      publisher.publish(pubsubMessage);
                    }

                    publisher.shutdown();
                  }
                });
          }
        });
  }

  @Override
  public void validateInput() {
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
  }
}
