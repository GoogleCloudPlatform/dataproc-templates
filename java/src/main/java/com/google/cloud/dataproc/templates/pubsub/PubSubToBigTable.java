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

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.dataproc.templates.BaseTemplate;
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

public class PubSubToBigTable implements BaseTemplate {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubToBigTable.class);
  private String inputProjectID;
  private String pubsubInputSubscription;
  private long timeoutMs;
  private int streamingDuration;
  private int totalReceivers;
  private String pubSubBigTableOutputInstanceId;
  private String pubSubBigTableOutputProjectId;
  private String pubSubBigTableOutputTable;

  public PubSubToBigTable() {
    inputProjectID = getProperties().getProperty(PUBSUB_INPUT_PROJECT_ID_PROP);
    pubsubInputSubscription = getProperties().getProperty(PUBSUB_INPUT_SUBSCRIPTION_PROP);
    timeoutMs = Long.parseLong(getProperties().getProperty(PUBSUB_TIMEOUT_MS_PROP));
    streamingDuration =
        Integer.parseInt(getProperties().getProperty(PUBSUB_STREAMING_DURATION_SECONDS_PROP));
    totalReceivers = Integer.parseInt(getProperties().getProperty(PUBSUB_TOTAL_RECEIVERS_PROP));
    pubSubBigTableOutputInstanceId =
        getProperties().getProperty(PUBSUB_BIGTABLE_OUTPUT_INSTANCE_ID_PROP);
    pubSubBigTableOutputProjectId =
        getProperties().getProperty(PUBSUB_BIGTABLE_OUTPUT_PROJECT_ID_PROP);
    pubSubBigTableOutputTable = getProperties().getProperty(PUBSUB_BIGTABLE_OUTPUT_TABLE_PROP);
  }

  @Override
  public void runTemplate() {

    validateInput();

    JavaStreamingContext jsc = null;

    try {
      SparkConf sparkConf = new SparkConf().setAppName("PubSubToBigTable Dataproc Job");
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

      LOGGER.info("Writing data to outputPath: {}", pubSubBigTableOutputTable);

      writeToBigTable(
          stream,
          pubSubBigTableOutputInstanceId,
          pubSubBigTableOutputProjectId,
          pubSubBigTableOutputTable);

      jsc.start();
      jsc.awaitTerminationOrTimeout(timeoutMs);

      LOGGER.info("PubSubToBigTable job completed.");
      jsc.stop();
    } catch (Throwable th) {
      LOGGER.error("Exception in PubSubToBTable", th);
      if (Objects.nonNull(jsc)) {
        jsc.stop();
      }
    }
  }

  public static void writeToBigTable(
      JavaDStream<SparkPubsubMessage> pubSubStream,
      String pubSubBigTableOutputInstanceId,
      String pubSubBigTableOutputProjectId,
      String pubSubBigTableOutputTable) {
    pubSubStream.foreachRDD(
        new VoidFunction<JavaRDD<SparkPubsubMessage>>() {
          @Override
          public void call(JavaRDD<SparkPubsubMessage> sparkPubsubMessageJavaRDD) throws Exception {
            sparkPubsubMessageJavaRDD.foreachPartition(
                new VoidFunction<Iterator<SparkPubsubMessage>>() {
                  @Override
                  public void call(Iterator<SparkPubsubMessage> sparkPubsubMessageIterator)
                      throws Exception {

                    BigtableDataClient dataClient =
                        BigtableDataClient.create(
                            pubSubBigTableOutputProjectId, pubSubBigTableOutputInstanceId);

                    while (sparkPubsubMessageIterator.hasNext()) {
                      SparkPubsubMessage message = sparkPubsubMessageIterator.next();

                      JSONObject record = new JSONObject(new String(message.getData()));
                      long timestamp = System.currentTimeMillis() * 1000;

                      RowMutation rowMutation =
                          RowMutation.create(pubSubBigTableOutputTable, record.getString(ROWKEY));
                      JSONArray columnarray = record.getJSONArray(COLUMNS);

                      for (int i = 0; i < columnarray.length(); i++) {
                        rowMutation.setCell(
                            columnarray.getJSONObject(i).getString(COLUMN_FAMILY),
                            columnarray.getJSONObject(i).getString(COLUMN_NAME),
                            timestamp,
                            columnarray.getJSONObject(i).getString(COLUMN_VALUE));
                      }
                      dataClient.mutateRow(rowMutation);
                    }

                    dataClient.close();
                  }
                });
          }
        });
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(inputProjectID)
        || StringUtils.isAllBlank(pubsubInputSubscription)
        || StringUtils.isAllBlank(pubSubBigTableOutputInstanceId)
        || StringUtils.isAllBlank(pubSubBigTableOutputProjectId)
        || StringUtils.isAllBlank(pubSubBigTableOutputTable)) {
      LOGGER.error(
          "{},{},{},{},{} are required parameter. ",
          PUBSUB_INPUT_PROJECT_ID_PROP,
          PUBSUB_INPUT_SUBSCRIPTION_PROP,
          PUBSUB_BIGTABLE_OUTPUT_INSTANCE_ID_PROP,
          PUBSUB_BIGTABLE_OUTPUT_PROJECT_ID_PROP,
          PUBSUB_BIGTABLE_OUTPUT_TABLE_PROP);
      throw new IllegalArgumentException(
          "Required parameters for PubSubToBigTable not passed. "
              + "Set mandatory parameter for PubSubToBigTable template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting PubSub to BigTable spark job with following parameters:"
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
        PUBSUB_BIGTABLE_OUTPUT_INSTANCE_ID_PROP,
        pubSubBigTableOutputInstanceId,
        PUBSUB_BIGTABLE_OUTPUT_PROJECT_ID_PROP,
        pubSubBigTableOutputProjectId,
        PUBSUB_BIGTABLE_OUTPUT_TABLE_PROP,
        pubSubBigTableOutputTable);
  }
}
