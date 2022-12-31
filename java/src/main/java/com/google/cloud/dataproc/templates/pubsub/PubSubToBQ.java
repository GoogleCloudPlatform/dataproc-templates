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
package com.google.cloud.dataproc.templates.pubsub;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.spark.bigquery.repackaged.com.google.api.core.ApiFuture;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.*;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1beta2.AppendRowsResponse;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1beta2.JsonStreamWriter;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.storage.v1beta2.TableName;
import com.google.cloud.spark.bigquery.repackaged.org.json.JSONArray;
import com.google.cloud.spark.bigquery.repackaged.org.json.JSONObject;
import java.util.Iterator;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.pubsub.PubsubUtils;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;
import org.apache.spark.streaming.pubsub.SparkPubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubToBQ implements BaseTemplate {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(com.google.cloud.dataproc.templates.pubsub.PubSubToBQ.class);
  private String inputProjectID;
  private String pubsubInputSubscription;
  private long timeoutMs;
  private int streamingDuration;
  private int totalReceivers;
  private String outputProjectID;
  private String pubSubBQOutputDataset;
  private String pubSubBQOutputTable;
  private int batchSize;
  private final String sparkLogLevel;

  public PubSubToBQ() {
    inputProjectID = getProperties().getProperty(PUBSUB_INPUT_PROJECT_ID_PROP);
    pubsubInputSubscription = getProperties().getProperty(PUBSUB_INPUT_SUBSCRIPTION_PROP);
    timeoutMs = Long.parseLong(getProperties().getProperty(PUBSUB_TIMEOUT_MS_PROP));
    streamingDuration =
        Integer.parseInt(getProperties().getProperty(PUBSUB_STREAMING_DURATION_SECONDS_PROP));
    totalReceivers = Integer.parseInt(getProperties().getProperty(PUBSUB_TOTAL_RECEIVERS_PROP));
    outputProjectID = getProperties().getProperty(PUBSUB_BQ_OUTPUT_PROJECT_ID_PROP);
    pubSubBQOutputDataset = getProperties().getProperty(PUBSUB_BQ_OUTPOUT_DATASET_PROP);
    pubSubBQOutputTable = getProperties().getProperty(PUBSUB_BQ_OUTPOUT_TABLE_PROP);
    batchSize = Integer.parseInt(getProperties().getProperty(PUBSUB_BQ_BATCH_SIZE_PROP));
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);

  }

  @Override
  public void runTemplate() {

    validateInput();

    JavaStreamingContext jsc = null;

    try {
      SparkConf sparkConf = new SparkConf().setAppName("PubSubToBigQuery Dataproc Job");
      jsc = new JavaStreamingContext(sparkConf, Seconds.apply(streamingDuration));
      
      // Set log level
      jsc.sparkContext().setLogLevel(sparkLogLevel);

      JavaDStream<SparkPubsubMessage> stream = null;
      for (int i = 0; i < totalReceivers; i += 1) {
        JavaDStream<SparkPubsubMessage> pubSubReciever =
            PubsubUtils.createStream(
                jsc,
                inputProjectID,
                pubsubInputSubscription,
                new SparkGCPCredentials.Builder().build(),
                StorageLevel.MEMORY_AND_DISK_SER());
        if (stream == null) {
          stream = pubSubReciever;
        } else {
          stream = stream.union(pubSubReciever);
        }
      }

      LOGGER.info("Writing data to outputPath: {}", pubSubBQOutputTable);
      writeToBQ(stream, inputProjectID, pubSubBQOutputDataset, pubSubBQOutputTable, batchSize);

      jsc.start();
      jsc.awaitTerminationOrTimeout(timeoutMs);

      LOGGER.info("PubSubToBq job completed.");
      jsc.stop();

    } catch (Throwable th) {
      LOGGER.error("Exception in PubSubToBQ", th);
      if (Objects.nonNull(jsc)) {
        jsc.stop();
      }
    }
  }

  public static void writeToBQ(
      JavaDStream<SparkPubsubMessage> pubSubStream,
      String outputProjectID,
      String pubSubBQOutputDataset,
      String PubSubBQOutputTable,
      Integer batchSize) {
    pubSubStream.foreachRDD(
        new VoidFunction<JavaRDD<SparkPubsubMessage>>() {
          @Override
          public void call(JavaRDD<SparkPubsubMessage> sparkPubsubMessageJavaRDD) throws Exception {
            sparkPubsubMessageJavaRDD.foreachPartition(
                new VoidFunction<Iterator<SparkPubsubMessage>>() {
                  @Override
                  public void call(Iterator<SparkPubsubMessage> sparkPubsubMessageIterator)
                      throws Exception {
                    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
                    Table table = bigquery.getTable(pubSubBQOutputDataset, PubSubBQOutputTable);
                    TableName parentTable =
                        TableName.of(outputProjectID, pubSubBQOutputDataset, PubSubBQOutputTable);
                    Schema schema = table.getDefinition().getSchema();
                    JsonStreamWriter writer =
                        JsonStreamWriter.newBuilder(parentTable.toString(), schema).build();

                    JSONArray jsonArr = new JSONArray();
                    while (sparkPubsubMessageIterator.hasNext()) {
                      SparkPubsubMessage message = sparkPubsubMessageIterator.next();
                      JSONObject record = new JSONObject(new String(message.getData()));
                      jsonArr.put(record);
                      if (jsonArr.length() == batchSize) {
                        ApiFuture<AppendRowsResponse> future = writer.append(jsonArr);
                        AppendRowsResponse response = future.get();
                        jsonArr = new JSONArray();
                      }
                    }
                    if (jsonArr.length() > 0) {
                      ApiFuture<AppendRowsResponse> future = writer.append(jsonArr);
                      AppendRowsResponse response = future.get();
                    }
                    writer.close();
                  }
                });
          }
        });
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(inputProjectID)
        || StringUtils.isAllBlank(pubsubInputSubscription)
        || StringUtils.isAllBlank(outputProjectID)
        || StringUtils.isAllBlank(pubSubBQOutputDataset)
        || StringUtils.isAllBlank(pubSubBQOutputTable)) {
      LOGGER.error(
          "{},{},{},{},{} are required parameter. ",
          PUBSUB_INPUT_PROJECT_ID_PROP,
          PUBSUB_INPUT_SUBSCRIPTION_PROP,
          PUBSUB_BQ_OUTPUT_PROJECT_ID_PROP,
          PUBSUB_BQ_OUTPOUT_DATASET_PROP,
          PUBSUB_BQ_OUTPOUT_TABLE_PROP);
      throw new IllegalArgumentException(
          "Required parameters for PubSubToBQ not passed. "
              + "Set mandatory parameter for PubSubToBQ template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting Hive to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5. {},{}"
            + "6. {},{}"
            + "7. {},{}"
            + "8. {},{}"
            + "9, {},{}",
        PUBSUB_INPUT_PROJECT_ID_PROP,
        inputProjectID,
        PUBSUB_INPUT_SUBSCRIPTION_PROP,
        pubsubInputSubscription,
        PUBSUB_TIMEOUT_MS_PROP,
        timeoutMs,
        PUBSUB_STREAMING_DURATION_SECONDS_PROP,
        streamingDuration,
        PUBSUB_TOTAL_RECEIVERS_PROP,
        totalReceivers,
        PUBSUB_BQ_OUTPUT_PROJECT_ID_PROP,
        outputProjectID,
        PUBSUB_BQ_OUTPOUT_DATASET_PROP,
        pubSubBQOutputDataset,
        PUBSUB_BQ_OUTPOUT_TABLE_PROP,
        pubSubBQOutputTable,
        PUBSUB_STREAMING_DURATION_SECONDS_PROP,
        batchSize);
  }
}
