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
import com.google.cloud.dataproc.templates.pubsub.internal.PubSubAcker;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubToGCS implements BaseTemplate {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(com.google.cloud.dataproc.templates.pubsub.PubSubToGCS.class);
  private final PubSubToGCSConfig pubSubToGCSConfig;

  private volatile long lastActivityTime = System.currentTimeMillis();

  public static PubSubToGCS of(String... args) {
    PubSubToGCSConfig pubSubToGCSConfig =
        PubSubToGCSConfig.fromProperties(PropertyUtil.getProperties());
    LOGGER.info("Config loaded\n{}", pubSubToGCSConfig);
    return new PubSubToGCS(pubSubToGCSConfig);
  }

  public PubSubToGCS(PubSubToGCSConfig pubSubToGCSConfig) {
    this.pubSubToGCSConfig = pubSubToGCSConfig;
  }

  @Override
  public void validateInput() throws IllegalArgumentException {
    ValidationUtil.validateOrThrow(pubSubToGCSConfig);
  }

  @Override
  public void runTemplate() throws InterruptedException, TimeoutException {

    LOGGER.info("Initialize Spark Session");
    SparkSession sparkSession =
        SparkSession.builder().appName("PubSubToGCS Dataproc Job").getOrCreate();

    LOGGER.info("Set Log Level {}", pubSubToGCSConfig.getSparkLogLevel());
    sparkSession.sparkContext().setLogLevel(pubSubToGCSConfig.getSparkLogLevel());

    LOGGER.info("Prepare Properties");
    Map<String, String> pubsubOptions = new HashMap<>();
    pubsubOptions.put("projectId", pubSubToGCSConfig.getInputProjectID());
    pubsubOptions.put("subscriptionId", pubSubToGCSConfig.getPubsubInputSubscription());
    String batchSize =
        pubSubToGCSConfig.getBatchSize() <= 0
            ? "1000"
            : String.valueOf(pubSubToGCSConfig.getBatchSize());
    pubsubOptions.put("maxMessagesPerPull", batchSize);
    String totalReceivers =
        pubSubToGCSConfig.getTotalReceivers() <= 0
            ? "4"
            : String.valueOf(pubSubToGCSConfig.getTotalReceivers());
    pubsubOptions.put("numPartitions", totalReceivers);

    LOGGER.info("Spark Streaming Configs: {}", pubsubOptions);
    long timeoutMs =
        pubSubToGCSConfig.getTimeoutMs() <= 0 ? 2000L : pubSubToGCSConfig.getTimeoutMs();

    try {

      LOGGER.info("Config GCS Bucket");
      String gcsBucket;
      if(pubSubToGCSConfig.getGcsBucketName().startsWith("gs://")){
        gcsBucket = pubSubToGCSConfig.getGcsBucketName();
      }else{
        gcsBucket = "gs://" + pubSubToGCSConfig.getGcsBucketName();
      }

      LOGGER.info("Starting Spark Read Stream");
      Dataset<Row> dataset =
          sparkSession
              .readStream()
              .format(PUBSUB_DATASOURCE_SHORT_NAME)
              .options(pubsubOptions)
              .load();

      LOGGER.info("Start Writing Data");
      StreamingQuery streamingQuery =
          dataset
              .writeStream()
              .queryName("PubSubToGCSStreamingQuery")
              .foreachBatch(
                  (df, batchId) -> {
                    LOGGER.info("Processing Batch ID: {}", batchId);
                    if (!df.isEmpty()) {

                      LOGGER.info("Data is available to write for batch id: {}", batchId);
                      Dataset<Row> df_data = df.select("data");

                      switch (pubSubToGCSConfig.getOutputDataFormat().toLowerCase()) {
                        case PUBSUB_GCS_AVRO_EXTENSION:
                          df_data
                              .write()
                              .mode(SaveMode.Append)
                              .format("avro")
                              .save(gcsBucket);
                          break;

                        case PUBSUB_GCS_JSON_EXTENSION:
                          df_data
                              .write()
                              .mode(SaveMode.Append)
                              .json(gcsBucket);
                          break;

                        default:
                          LOGGER.info(
                              "Unsupported DataFormat {}. Discarding current dataset.",
                              pubSubToGCSConfig.getOutputDataFormat());
                          break;
                      }

                      LOGGER.info("Start Acknowledgement For Batch ID: {}", batchId);
                      PubSubAcker.acknowledge(df, pubsubOptions);

                      lastActivityTime = System.currentTimeMillis();

                    } else {
                      LOGGER.info("No data available for batch id: {}", batchId);
                    }

                    long currentTime = System.currentTimeMillis();
                    long inactiveDurationMillis = currentTime - lastActivityTime;

                    if (inactiveDurationMillis > timeoutMs) {
                      LOGGER.info(
                          "No new messages for {} milliseconds. Stopping stream...", timeoutMs);
                      LOGGER.info("Throwing StreamingQueryException to stop the query gracefully");
                      throw new StreamingQueryException(
                          "Inactivity timeout reached, stopping stream.",
                          "Inactivity timeout reached, stopping stream.",
                          new Exception("Inactivity timeout reached, stopping stream."),
                          null,
                          null,
                          null,
                          null);
                    }
                  })
              .start();

      try {
        streamingQuery.awaitTermination();
      } catch (StreamingQueryException e) {
        LOGGER.error("Streaming Query stopped due to : ", e);
      }

      LOGGER.info("Spark Session Stop");
      sparkSession.stop();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
