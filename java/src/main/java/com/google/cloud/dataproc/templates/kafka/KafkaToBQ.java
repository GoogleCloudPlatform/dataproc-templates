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
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToBQ implements BaseTemplate {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToBQ.class);
  private String projectId;
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private String checkpointLocation;
  private String kafkaStartingOffsets;
  private Long kafkaAwaitTerminationTimeout;
  private String failOnDataLoss;
  private String bigQueryDataset;
  private String bigQueryTable;
  private String tempGcsBucket;
  private String streamOutputMode;
  private final String sparkLogLevel;

  public KafkaToBQ() {
    projectId = getProperties().getProperty(PROJECT_ID_PROP);
    kafkaBootstrapServers = getProperties().getProperty(KAFKA_BQ_BOOTSTRAP_SERVERS);
    kafkaTopic = getProperties().getProperty(KAFKA_BQ_TOPIC);
    checkpointLocation = getProperties().getProperty(KAFKA_BQ_CHECKPOINT_LOCATION);
    kafkaStartingOffsets = getProperties().getProperty(KAFKA_BQ_STARTING_OFFSET);
    failOnDataLoss = getProperties().getProperty(KAFKA_BQ_FAIL_ON_DATA_LOSS);
    bigQueryDataset = getProperties().getProperty(KAFKA_BQ_DATASET);
    bigQueryTable = getProperties().getProperty(KAFKA_BQ_TABLE);
    tempGcsBucket = getProperties().getProperty(KAFKA_BQ_TEMP_GCS_BUCKET);
    kafkaAwaitTerminationTimeout =
        Long.valueOf(getProperties().getProperty(KAFKA_BQ_AWAIT_TERMINATION_TIMEOUT));
    streamOutputMode = getProperties().getProperty(KAFKA_BQ_STREAM_OUTPUT_MODE);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() throws StreamingQueryException, TimeoutException {
    validateInput();

    // Initialize the Spark session
    SparkSession spark = SparkSession.builder().appName("Spark KafkaToBQ Job").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    // Stream data from Kafka topic
    Dataset<Row> inputData =
        spark
            .readStream()
            .format(KAFKA_BQ_SPARK_CONF_NAME_INPUT_FORMAT)
            .option(KAFKA_BQ_SPARK_CONF_NAME_BOOTSTRAP_SERVERS, kafkaBootstrapServers)
            .option(KAFKA_BQ_SPARK_CONF_NAME_SUBSCRIBE, kafkaTopic)
            .option(KAFKA_BQ_SPARK_CONF_NAME_STARTING_OFFSETS, kafkaStartingOffsets)
            .option(KAFKA_BQ_SPARK_CONF_NAME_FAIL_ON_DATA_LOSS, failOnDataLoss)
            .load();

    Dataset<Row> processedData =
        inputData
            .withColumn("value", inputData.col("value").cast(DataTypes.StringType))
            .withColumn("key", inputData.col("key").cast(DataTypes.StringType));

    writeToBigQuery(
        processedData,
        streamOutputMode,
        checkpointLocation,
        projectId,
        bigQueryDataset,
        bigQueryTable,
        tempGcsBucket,
        kafkaAwaitTerminationTimeout);

    LOGGER.info("KafkaToBQ job completed.");
    spark.stop();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(checkpointLocation)
        || StringUtils.isAllBlank(kafkaBootstrapServers)
        || StringUtils.isAllBlank(kafkaTopic)
        || StringUtils.isAllBlank(bigQueryDataset)
        || StringUtils.isAllBlank(bigQueryTable)
        || StringUtils.isAllBlank(projectId)
        || StringUtils.isAllBlank(tempGcsBucket)) {
      LOGGER.error(
          "{},{},{},{},{},{},{} is required parameter. ",
          PROJECT_ID_PROP,
          KAFKA_BQ_CHECKPOINT_LOCATION,
          KAFKA_BQ_BOOTSTRAP_SERVERS,
          KAFKA_BQ_TOPIC,
          KAFKA_BQ_DATASET,
          KAFKA_BQ_TABLE,
          KAFKA_BQ_TEMP_GCS_BUCKET);
      throw new IllegalArgumentException(
          "Required parameters for KafkaToBQ not passed. "
              + "Set mandatory parameter for KafkaToBQ template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting Kafka to BQ spark job with following parameters:"
            + "1. {}:{} "
            + "2. {}:{} "
            + "3. {}:{} "
            + "4. {},{} "
            + "5. {},{} "
            + "6. {},{} "
            + "7. {},{} "
            + "8. {},{} ",
        KAFKA_BQ_CHECKPOINT_LOCATION,
        checkpointLocation,
        KAFKA_BQ_BOOTSTRAP_SERVERS,
        kafkaBootstrapServers,
        KAFKA_BQ_TOPIC,
        kafkaTopic,
        KAFKA_BQ_STARTING_OFFSET,
        kafkaStartingOffsets,
        KAFKA_BQ_AWAIT_TERMINATION_TIMEOUT,
        kafkaAwaitTerminationTimeout,
        KAFKA_BQ_DATASET,
        bigQueryDataset,
        KAFKA_BQ_TABLE,
        bigQueryTable,
        KAFKA_BQ_TEMP_GCS_BUCKET,
        tempGcsBucket);
  }

  public void writeToBigQuery(
      Dataset<Row> dataset,
      String streamOutputMode,
      String checkpointLocation,
      String projectId,
      String bigQueryDataset,
      String bigQueryTable,
      String tempGcsBucket,
      Long kafkaAwaitTerminationTimeout)
      throws StreamingQueryException, TimeoutException {
    dataset
        .writeStream()
        .format(KAFKA_BQ_SPARK_CONF_NAME_OUTPUT_FORMAT)
        .outputMode(streamOutputMode)
        .option(KAFKA_BQ_SPARK_CONF_NAME_OUTPUT_HEADER, true)
        .option(KAFKA_BQ_SPARK_CONF_NAME_CHECKPOINT_LOCATION, checkpointLocation)
        .option(
            KAFKA_BQ_SPARK_CONF_NAME_TABLE, projectId + "." + bigQueryDataset + "." + bigQueryTable)
        .option(KAFKA_BQ_SPARK_CONF_NAME_TEMP_GCS_BUCKET, tempGcsBucket)
        .start()
        .awaitTermination(kafkaAwaitTerminationTimeout);
  }
}
