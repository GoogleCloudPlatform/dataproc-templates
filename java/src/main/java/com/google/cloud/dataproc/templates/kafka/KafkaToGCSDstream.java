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
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Spark job to move data or/and schema from Kafka topic to GCS via spark Direct Stream. This
 * template can be configured to run in few different modes. In default mode kafka.gcs.write.mode is
 * set to "append". For detailed list of properties refer "KafkaToGCSDstream Template properties"
 * section in resources/template.properties file.
 */
public class KafkaToGCSDstream implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(KafkaToGCSDstream.class);

  private long batchInterval;
  private String kafkaMessageFormat;
  private String gcsOutputLocation;
  private String gcsWriteMode;
  private String gcsOutputFormat;
  private String kafkaStartingOffsets;
  private String kafkaGroupId;
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private final String sparkLogLevel;
  private final String kafkaSchemaUrl;
  private Long kafkaAwaitTerminationTimeout;

  public KafkaToGCSDstream() {

    kafkaBootstrapServers = getProperties().getProperty(KAFKA_BOOTSTRAP_SERVERS);
    kafkaTopic = getProperties().getProperty(KAFKA_TOPIC);
    kafkaMessageFormat = getProperties().getProperty(KAFKA_MESSAGE_FORMAT);
    gcsOutputLocation = getProperties().getProperty(KAFKA_GCS_OUTPUT_LOCATION);
    gcsOutputFormat = getProperties().getProperty(KAFKA_GCS_OUTPUT_FORMAT);
    kafkaStartingOffsets = getProperties().getProperty(KAFKA_STARTING_OFFSET);
    kafkaGroupId = getProperties().getProperty(KAFKA_GCS_CONSUMER_GROUP_ID);
    batchInterval = Long.parseLong(getProperties().getProperty(KAFKA_GCS_BATCH_INTERVAL));
    gcsWriteMode = getProperties().getProperty(KAFKA_GCS_WRITE_MODE);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
    kafkaSchemaUrl = getProperties().getProperty(KAFKA_SCHEMA_URL);
    kafkaAwaitTerminationTimeout =
        Long.valueOf(getProperties().getProperty(KAFKA_GCS_AWAIT_TERMINATION_TIMEOUT));
  }

  @Override
  public void runTemplate() throws TimeoutException, SQLException, InterruptedException {

    SparkSession spark =
        SparkSession.builder().appName("Kafka to GCS via Direct stream").getOrCreate();

    spark.sparkContext().setLogLevel(sparkLogLevel);

    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", kafkaBootstrapServers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", kafkaGroupId);
    kafkaParams.put("auto.offset.reset", kafkaStartingOffsets);
    kafkaParams.put("enable.auto.commit", false);

    Collection<String> topics = Collections.singletonList(kafkaTopic);

    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

    JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, new Duration(batchInterval));
    KafkaReader reader = new KafkaReader();

    JavaInputDStream<ConsumerRecord<Object, Object>> stream =
        KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topics, kafkaParams));

    stream.foreachRDD(
        (VoidFunction2<JavaRDD<ConsumerRecord<Object, Object>>, Time>)
            (rdd, time) -> {
              LOGGER.debug("Reading kafka data");

              OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

              JavaRDD<Tuple2<String, String>> recordRdd =
                  rdd.map(record -> new Tuple2(record.key(), record.value()));

              Dataset<Row> rowDataset =
                  spark
                      .createDataset(
                          recordRdd.rdd(), Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
                      .withColumnRenamed("_1", "key")
                      .withColumnRenamed("_2", "value");

              Dataset<Row> processedData =
                  reader.getDatasetByMessageFormat(rowDataset, getProperties());

              if (!processedData.isEmpty()) {
                DataFrameWriter<Row> writer =
                    processedData.write().mode(gcsWriteMode).format(gcsOutputFormat);

                LOGGER.debug("Writing kafka data into GCS");
                writer.format(gcsOutputFormat).save(gcsOutputLocation);
              }
              ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
            });

    ssc.start();
    ssc.awaitTerminationOrTimeout(kafkaAwaitTerminationTimeout);
  }

  @Override
  public void validateInput() throws IllegalArgumentException {
    if (StringUtils.isAllBlank(gcsOutputLocation)
        || StringUtils.isAllBlank(gcsOutputFormat)
        || StringUtils.isAllBlank(gcsWriteMode)
        || StringUtils.isAllBlank(kafkaBootstrapServers)
        || StringUtils.isAllBlank(kafkaTopic)
        || StringUtils.isAllBlank(kafkaMessageFormat)
        || StringUtils.isAllBlank(kafkaGroupId)) {
      LOGGER.error(
          "{},{},{},{},{},{},{} are required parameter. ",
          KAFKA_GCS_OUTPUT_LOCATION,
          KAFKA_GCS_OUTPUT_FORMAT,
          KAFKA_GCS_WRITE_MODE,
          KAFKA_BOOTSTRAP_SERVERS,
          KAFKA_TOPIC,
          KAFKA_MESSAGE_FORMAT,
          KAFKA_GCS_CONSUMER_GROUP_ID);
      throw new IllegalArgumentException(
          "Required parameters for KafkaTOGCSDstream not passed. "
              + "Set mandatory parameter for KafkaTOGCSDstream template "
              + "in resources/conf/template.properties file.");
    }

    if (kafkaMessageFormat.equals("json") & StringUtils.isAllBlank(kafkaSchemaUrl)) {
      LOGGER.error("{} is a required parameter for JSON format messages", KAFKA_SCHEMA_URL);
      throw new IllegalArgumentException("Required parameters for KafkaToGCSDstream not passed.");
    }

    LOGGER.info(
        "Starting kafka to GCS via DStream spark job with following parameters:"
            + "1. {}:{} "
            + "2. {}:{} "
            + "3. {}:{} "
            + "4. {}:{} "
            + "5. {}:{} "
            + "6. {}:{} "
            + "7. {}:{} "
            + "8. {}:{} "
            + "9. {}:{} "
            + "10. {}:{} ",
        KAFKA_MESSAGE_FORMAT,
        kafkaMessageFormat,
        KAFKA_GCS_OUTPUT_LOCATION,
        gcsOutputLocation,
        KAFKA_GCS_OUTPUT_FORMAT,
        gcsOutputFormat,
        KAFKA_GCS_WRITE_MODE,
        gcsWriteMode,
        KAFKA_BOOTSTRAP_SERVERS,
        kafkaBootstrapServers,
        KAFKA_TOPIC,
        kafkaTopic,
        KAFKA_GCS_BATCH_INTERVAL,
        batchInterval,
        KAFKA_SCHEMA_URL,
        kafkaSchemaUrl,
        KAFKA_GCS_CONSUMER_GROUP_ID,
        kafkaGroupId,
        KAFKA_GCS_AWAIT_TERMINATION_TIMEOUT,
        kafkaAwaitTerminationTimeout);
  }
}
