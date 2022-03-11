/*
 * Copyright (C) 2022 Google LLC
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

public class PubSubToGCS implements BaseTemplate {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(com.google.cloud.dataproc.templates.pubsub.PubSubToGCS.class);
  private String inputProjectID;
  private String pubsubInputSubscription;
  private long timeoutMs;
  private int streamingDuration;
  private int totalReceivers;
  private String outputProjectID;
  private int batchSize;

  public PubSubToGCS() {
    inputProjectID = getProperties().getProperty(PUBSUB_GCS_INPUT_PROJECT_ID_PROP);
    pubsubInputSubscription = getProperties().getProperty(PUBSUB_GCS_INPUT_SUBSCRIPTION_PROP);
    timeoutMs = Long.parseLong(getProperties().getProperty(PUBSUB_GCS_TIMEOUT_MS_PROP));
    streamingDuration =
        Integer.parseInt(getProperties().getProperty(PUBSUB_GCS_STREAMING_DURATION_SECONDS_PROP));
    totalReceivers = Integer.parseInt(getProperties().getProperty(PUBSUB_GCS_TOTAL_RECEIVERS_PROP));
    outputProjectID = getProperties().getProperty(PUBSUB_GCS_OUTPUT_PROJECT_ID_PROP);
    gcsBucketUrl = Integer.parseInt(getProperties().getProperty(PUBSUB_GCS_BUCKET_URL));
    batchSize = Integer.parseInt(getProperties().getProperty(PUBSUB_GCS_BATCH_SIZE_PROP));
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(inputProjectID)
        || StringUtils.isAllBlank(pubsubInputSubscription)
        || StringUtils.isAllBlank(outputProjectID)
        || StringUtils.isAllBlank(gcsBucketUrl)) {
      LOGGER.error(
          "{},{},{},{},{} are required parameter. ",
          PUBSUB_GCS_INPUT_PROJECT_ID_PROP,
          PUBSUB_GCS_INPUT_SUBSCRIPTION_PROP,
          PUBSUB_GCS_OUTPUT_PROJECT_ID_PROP,
          PUBSUB_GCS_BUCKET_URL);
      throw new IllegalArgumentException(
          "Required parameters for PubSubToGCS not passed. "
              + "Set mandatory parameter for PubSubToGCS template "
              + "in resources/conf/template.properties file.");
    }

    JavaStreamingContext jsc = null;
    LOGGER.info(
        "Starting PubSub to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5. {},{}"
            + "6. {},{}"
            + "7. {},{}"
            + "8. {},{}",
        PUBSUB_GCS_INPUT_PROJECT_ID_PROP,
        inputProjectID,
        PUBSUB_GCS_INPUT_SUBSCRIPTION_PROP,
        pubsubInputSubscription,
        PUBSUB_GCS_TIMEOUT_MS_PROP,
        timeoutMs,
        PUBSUB_GCS_STREAMING_DURATION_SECONDS_PROP,
        streamingDuration,
        PUBSUB_GCS_TOTAL_RECEIVERS_PROP,
        totalReceivers,
        PUBSUB_GCS_OUTPUT_PROJECT_ID_PROP,
        outputProjectID,
        PUBSUB_GCS_BUCKET_URL,
        gcsBucketUrl,
        PUBSUB_GCS_BATCH_SIZE_PROP,
        batchSize);

    try {
      SparkConf sparkConf = new SparkConf().setAppName("PubSubToGCS Dataproc Job");
      jsc = new JavaStreamingContext(sparkConf, Seconds.apply(streamingDuration));

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

      LOGGER.info("Writing data to output GCS Bucket: {}", gcsBucketUrl);
      writeToGCS(stream, outputProjectID, gcsBucketUrl, batchSize);

      jsc.start();
      jsc.awaitTerminationOrTimeout(timeoutMs);

      LOGGER.info("PubSubToGCS job completed.");
      jsc.stop();

    } catch (Throwable th) {
      LOGGER.error("Exception in PubSubToGCS", th);
      if (Objects.nonNull(jsc)) {
        jsc.stop();
      }
    }
  }

  public static void writeToGCS(
      JavaDStream<SparkPubsubMessage> pubSubStream,
      String outputProjectID,
      String gcsBucketUrl,
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


                    while (sparkPubsubMessageIterator.hasNext()) {
                      SparkPubsubMessage message = sparkPubsubMessageIterator.next();
                      print(new String(message.getData()))
                    }

                    /**
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
                    */
                    



                  }
                });
          }
        });
  }
}
