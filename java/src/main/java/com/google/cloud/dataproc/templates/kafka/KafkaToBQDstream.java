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
 * Spark job to move data or/and schema from Kafka topic to BQ via spark Direct Stream. For detailed
 * list of properties refer "KafkaToBQDstream Template properties" section in
 * resources/template.properties file.
 */
public class KafkaToBQDstream implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(KafkaToBQDstream.class);

  private String projectId;
  private long batchInterval;
  private String bqWriteMode;
  private String kafkaStartingOffsets;
  private String kafkaGroupId;
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private final String sparkLogLevel;
  private Long kafkaAwaitTerminationTimeout;
  private String bigQueryDataset;
  private String bigQueryTable;
  private String tempGcsBucket;

  public KafkaToBQDstream() {

    projectId = getProperties().getProperty(PROJECT_ID_PROP);
    kafkaBootstrapServers = getProperties().getProperty(KAFKA_BOOTSTRAP_SERVERS);
    kafkaTopic = getProperties().getProperty(KAFKA_TOPIC);
    kafkaStartingOffsets = getProperties().getProperty(KAFKA_STARTING_OFFSET);
    kafkaGroupId = getProperties().getProperty(KAFKA_BQ_CONSUMER_GROUP_ID);
    batchInterval = Long.parseLong(getProperties().getProperty(KAFKA_BQ_BATCH_INTERVAL));
    bqWriteMode = getProperties().getProperty(KAFKA_BQ_STREAM_OUTPUT_MODE);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
    bigQueryDataset = getProperties().getProperty(KAFKA_BQ_DATASET);
    bigQueryTable = getProperties().getProperty(KAFKA_BQ_TABLE);
    tempGcsBucket = getProperties().getProperty(KAFKA_BQ_TEMP_GCS_BUCKET);
    kafkaAwaitTerminationTimeout =
        Long.valueOf(getProperties().getProperty(KAFKA_BQ_AWAIT_TERMINATION_TIMEOUT));
  }

  @Override
  public void runTemplate() throws TimeoutException, SQLException, InterruptedException {

    SparkSession spark =
        SparkSession.builder().appName("Kafka to BQ via Direct stream").getOrCreate();

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

              if (!rowDataset.isEmpty()) {

                LOGGER.info("Saving data into BQ");

                rowDataset
                    .write()
                    .mode(bqWriteMode)
                    .format("bigquery")
                    .option(
                        KAFKA_BQ_SPARK_CONF_NAME_TABLE,
                        projectId + "." + bigQueryDataset + "." + bigQueryTable)
                    .option(KAFKA_BQ_SPARK_CONF_NAME_TEMP_GCS_BUCKET, tempGcsBucket)
                    .option(KAFKA_BQ_SPARK_CONF_NAME_OUTPUT_HEADER, true)
                    .save();

                LOGGER.info("Saved to BQ");
              }
              ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
            });

    ssc.start();
    ssc.awaitTerminationOrTimeout(kafkaAwaitTerminationTimeout);
  }

  @Override
  public void validateInput() {
    if (StringUtils.isAllBlank(kafkaBootstrapServers)
        || StringUtils.isAllBlank(kafkaTopic)
        || StringUtils.isAllBlank(bigQueryDataset)
        || StringUtils.isAllBlank(bigQueryTable)
        || StringUtils.isAllBlank(projectId)
        || StringUtils.isAllBlank(tempGcsBucket)
        || StringUtils.isAllBlank(kafkaGroupId)
        || StringUtils.isAllBlank(bqWriteMode)) {
      LOGGER.error(
          "{},{},{},{},{},{},{},{} is required parameter. ",
          PROJECT_ID_PROP,
          KAFKA_BOOTSTRAP_SERVERS,
          KAFKA_TOPIC,
          KAFKA_BQ_DATASET,
          KAFKA_BQ_TABLE,
          KAFKA_BQ_TEMP_GCS_BUCKET,
          KAFKA_BQ_CONSUMER_GROUP_ID,
          KAFKA_BQ_STREAM_OUTPUT_MODE);

      throw new IllegalArgumentException(
          "Required parameters for KafkaToBQDstream not passed. "
              + "Set mandatory parameter for KafkaToBQDstream template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting Kafka to BQ via DStream spark job with following parameters:"
            + "1. {}:{} "
            + "2. {}:{} "
            + "3. {}:{} "
            + "4. {},{} "
            + "5. {},{} "
            + "6. {},{} "
            + "7. {},{} "
            + "8. {},{} "
            + "9. {},{} "
            + "10. {},{} ",
        KAFKA_BOOTSTRAP_SERVERS,
        kafkaBootstrapServers,
        KAFKA_TOPIC,
        kafkaTopic,
        KAFKA_STARTING_OFFSET,
        kafkaStartingOffsets,
        KAFKA_BQ_AWAIT_TERMINATION_TIMEOUT,
        kafkaAwaitTerminationTimeout,
        KAFKA_BQ_DATASET,
        bigQueryDataset,
        KAFKA_BQ_TABLE,
        bigQueryTable,
        KAFKA_BQ_TEMP_GCS_BUCKET,
        tempGcsBucket,
        KAFKA_BQ_BATCH_INTERVAL,
        batchInterval,
        KAFKA_BQ_CONSUMER_GROUP_ID,
        kafkaGroupId,
        KAFKA_BQ_STREAM_OUTPUT_MODE,
        bqWriteMode);
  }
}
