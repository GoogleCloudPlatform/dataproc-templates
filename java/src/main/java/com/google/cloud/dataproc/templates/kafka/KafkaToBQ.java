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

public class KafkaToBQ implements BaseTemplate {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToBQ.class);
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private String checkpointLocation;
  private String kafkaStartingOffsets;
  private Long kafkaAwaitTerminationTimeout;

  public KafkaToBQ() {
    kafkaBootstrapServers = getProperties().getProperty(KAFKA_BQ_BOOTSTRAP_SERVERS);
    kafkaTopic = getProperties().getProperty(KAFKA_BQ_TOPIC);
    checkpointLocation = getProperties().getProperty(KAFKA_BQ_CHECKPOINT_LOCATION);
    kafkaStartingOffsets = getProperties().getProperty(KAFKA_BQ_STARTING_OFFSET);
    kafkaAwaitTerminationTimeout =
        Long.valueOf(getProperties().getProperty(KAFKA_BQ_AWAIT_TERMINATION_TIMEOUT));
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(checkpointLocation)
        || StringUtils.isAllBlank(kafkaBootstrapServers)
        || StringUtils.isAllBlank(kafkaTopic)) {
      LOGGER.error(
          "{},{},{} is required parameter. ",
          KAFKA_BQ_CHECKPOINT_LOCATION,
          KAFKA_BQ_BOOTSTRAP_SERVERS,
          KAFKA_BQ_TOPIC);
      throw new IllegalArgumentException(
          "Required parameters for KafkaToBQ not passed. "
              + "Set mandatory parameter for KafkaToBQ template "
              + "in resources/conf/template.properties file.");
    }

    SparkSession spark = null;
    LOGGER.info(
        "Starting Kafka to BQ spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5, {},{}",
        KAFKA_BQ_CHECKPOINT_LOCATION,
        checkpointLocation,
        KAFKA_BQ_BOOTSTRAP_SERVERS,
        kafkaBootstrapServers,
        KAFKA_BQ_TOPIC,
        kafkaTopic,
        KAFKA_BQ_STARTING_OFFSET,
        kafkaStartingOffsets,
        KAFKA_BQ_AWAIT_TERMINATION_TIMEOUT,
        kafkaAwaitTerminationTimeout);

    try {
      // Initialize the Spark session
      spark = SparkSession.builder().appName("Spark KafkaToBQ Job").getOrCreate();

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
          .writeStream()
          .format("com.google.cloud.spark.bigquery")
          .option("header", true)
          .option("checkpointLocation", checkpointLocation)
          .option("table", "yadavaja-sandbox.vbhatia_test.kafkatobq")
          .option("temporaryGcsBucket", "vbhatia_kafkatobq_tmp")
          .start()
          .awaitTermination(kafkaAwaitTerminationTimeout);

      LOGGER.info("KakfaToBQ job completed.");
      spark.stop();
    } catch (Throwable th) {
      LOGGER.error("Exception in KakfaToBQ", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
