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
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
import org.json.JSONArray;
import org.json.JSONObject;
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
  private String gcsBucketName;
  private int batchSize;
  private String outputDataFormat;
  private final String sparkLogLevel;

  public PubSubToGCS() {
    inputProjectID = getProperties().getProperty(PUBSUB_GCS_INPUT_PROJECT_ID_PROP);
    pubsubInputSubscription = getProperties().getProperty(PUBSUB_GCS_INPUT_SUBSCRIPTION_PROP);
    timeoutMs = Long.parseLong(getProperties().getProperty(PUBSUB_GCS_TIMEOUT_MS_PROP));
    streamingDuration =
        Integer.parseInt(getProperties().getProperty(PUBSUB_GCS_STREAMING_DURATION_SECONDS_PROP));
    totalReceivers = Integer.parseInt(getProperties().getProperty(PUBSUB_GCS_TOTAL_RECEIVERS_PROP));
    outputProjectID = getProperties().getProperty(PUBSUB_GCS_OUTPUT_PROJECT_ID_PROP);
    gcsBucketName = getProperties().getProperty(PUBSUB_GCS_BUCKET_NAME);
    batchSize = Integer.parseInt(getProperties().getProperty(PUBSUB_GCS_BATCH_SIZE_PROP));
    outputDataFormat = getProperties().getProperty(PUBSUB_GCS_OUTPUT_DATA_FORMAT);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() {
    validateInput();
    JavaStreamingContext jsc = null;
    try {
      SparkConf sparkConf = new SparkConf().setAppName("PubSubToGCS Dataproc Job");
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

      LOGGER.info("Writing data to output GCS Bucket: {}", gcsBucketName);
      Storage storage = StorageOptions.getDefaultInstance().getService();
      Bucket bucket = storage.get(gcsBucketName);
      writeToGCS(stream, outputProjectID, gcsBucketName, batchSize, outputDataFormat, bucket);

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

  public void validateInput() {
    if (StringUtils.isAllBlank(inputProjectID)
        || StringUtils.isAllBlank(pubsubInputSubscription)
        || StringUtils.isAllBlank(outputProjectID)
        || StringUtils.isAllBlank(gcsBucketName)
        || StringUtils.isAllBlank(outputDataFormat)) {
      LOGGER.error(
          "{},{},{},{},{} are required parameter. ",
          PUBSUB_GCS_INPUT_PROJECT_ID_PROP,
          PUBSUB_GCS_INPUT_SUBSCRIPTION_PROP,
          PUBSUB_GCS_OUTPUT_PROJECT_ID_PROP,
          PUBSUB_GCS_BUCKET_NAME,
          PUBSUB_GCS_OUTPUT_DATA_FORMAT);
      throw new IllegalArgumentException(
          "Required parameters for PubSubToGCS not passed. "
              + "Set mandatory parameter for PubSubToGCS template "
              + "in resources/conf/template.properties file.");
    }
    LOGGER.info(
        "Starting PubSub to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5. {},{}"
            + "6. {},{}"
            + "7. {},{}"
            + "8. {},{}"
            + "9. {},{}",
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
        PUBSUB_GCS_BUCKET_NAME,
        gcsBucketName,
        PUBSUB_GCS_BATCH_SIZE_PROP,
        batchSize,
        PUBSUB_GCS_OUTPUT_DATA_FORMAT,
        outputDataFormat);
  }

  public static void writeToGCS(
      JavaDStream<SparkPubsubMessage> pubSubStream,
      String outputProjectID,
      String gcsBucketName,
      Integer batchSize,
      String outputDataFormat,
      Bucket bucket) {
    pubSubStream.foreachRDD(
        new VoidFunction<JavaRDD<SparkPubsubMessage>>() {
          @Override
          public void call(JavaRDD<SparkPubsubMessage> sparkPubsubMessageJavaRDD) throws Exception {
            sparkPubsubMessageJavaRDD.foreachPartition(
                new VoidFunction<Iterator<SparkPubsubMessage>>() {
                  @Override
                  public void call(Iterator<SparkPubsubMessage> sparkPubsubMessageIterator)
                      throws Exception {
                    if (outputDataFormat.equalsIgnoreCase(PUBSUB_GCS_AVRO_EXTENSION)
                        || outputDataFormat.equalsIgnoreCase(PUBSUB_GCS_JSON_EXTENSION)) {
                      JSONArray jsonArr = new JSONArray();
                      while (sparkPubsubMessageIterator.hasNext()) {
                        SparkPubsubMessage message = sparkPubsubMessageIterator.next();
                        if (outputDataFormat.equalsIgnoreCase(PUBSUB_GCS_AVRO_EXTENSION)) {
                          writeAvroToGCS(message, bucket);
                        } else if (outputDataFormat.equalsIgnoreCase(PUBSUB_GCS_JSON_EXTENSION)) {
                          JSONObject record = new JSONObject(new String(message.getData()));
                          jsonArr.put(record);
                          if (jsonArr.length() == batchSize && jsonArr.length() > 0) {
                            writeJsonArrayToGCS(jsonArr, bucket);
                            jsonArr = new JSONArray();
                          }
                        }
                      } // end while
                      if (jsonArr.length() > 0
                          && outputDataFormat.equalsIgnoreCase(PUBSUB_GCS_JSON_EXTENSION)) {
                        // If any Json objects were missed including in the last batch
                        writeJsonArrayToGCS(jsonArr, bucket);
                        jsonArr = new JSONArray();
                      }
                    } else {
                      LOGGER.error(outputDataFormat + " is not supported...");
                    }
                  }
                });
          }
        });
  }

  public static void writeAvroToGCS(SparkPubsubMessage message, Bucket bucket) {
    LOGGER.info("PubSubToGCS message Avro start...");
    bucket.create(
        PUBSUB_GCS_BUCKET_OUTPUT_PATH + message.getMessageId() + "." + PUBSUB_GCS_AVRO_EXTENSION,
        message.getData());
    LOGGER.info("PubSubToGCS message Avro end...");
  }

  public static void writeJsonArrayToGCS(JSONArray jsonArr, Bucket bucket) {
    LOGGER.info("PubSubToGCS message Json Array start...");
    bucket.create(
        PUBSUB_GCS_BUCKET_OUTPUT_PATH
            + "batch_"
            + System.nanoTime()
            + "."
            + PUBSUB_GCS_JSON_EXTENSION,
        jsonArr.toString().getBytes(UTF_8));
    LOGGER.info("PubSubToGCS message Json Array end...");
  }
}
