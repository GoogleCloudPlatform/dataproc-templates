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
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.pubsub.internal.PubSubAcker;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubToBQ implements BaseTemplate {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(com.google.cloud.dataproc.templates.pubsub.PubSubToBQ.class);

  private final PubSubToBQConfig pubSubToBQConfig;
  private volatile long lastActivityTime = System.currentTimeMillis();

  public static PubSubToBQ of(String... args) {
    PubSubToBQConfig pubSubToBQConfig =
        PubSubToBQConfig.fromProperties(PropertyUtil.getProperties());
    LOGGER.info("Config loaded\n{}", pubSubToBQConfig);
    return new PubSubToBQ(pubSubToBQConfig);
  }

  public PubSubToBQ(PubSubToBQConfig pubSubToBQConfig) {
    this.pubSubToBQConfig = pubSubToBQConfig;
  }

  @Override
  public void validateInput() throws IllegalArgumentException {
    ValidationUtil.validateOrThrow(pubSubToBQConfig);
  }

  @Override
  public void runTemplate() throws InterruptedException {

    LOGGER.info("Initialize Spark Session");
    SparkSession sparkSession =
        SparkSession.builder().appName("PubSubToBQ Dataproc Job").getOrCreate();

    LOGGER.info("Set Log Level {}", pubSubToBQConfig.getSparkLogLevel());
    sparkSession.sparkContext().setLogLevel(pubSubToBQConfig.getSparkLogLevel());

    LOGGER.info("Prepare Properties");
    Map<String, String> pubsubOptions = new HashMap<>();
    pubsubOptions.put("projectId", pubSubToBQConfig.getInputProjectID());
    pubsubOptions.put("subscriptionId", pubSubToBQConfig.getPubsubInputSubscription());
    String batchSize =
        pubSubToBQConfig.getBatchSize() <= 0
            ? "1000"
            : String.valueOf(pubSubToBQConfig.getBatchSize());
    pubsubOptions.put("maxMessagesPerPull", batchSize);
    String totalReceivers =
        pubSubToBQConfig.getTotalReceivers() <= 0
            ? "4"
            : String.valueOf(pubSubToBQConfig.getTotalReceivers());
    pubsubOptions.put("numPartitions", totalReceivers);
    long timeoutMs = pubSubToBQConfig.getTimeoutMs() <= 0 ? 2000L : pubSubToBQConfig.getTimeoutMs();

    try {

      LOGGER.info("Retrieve BigQuery Table Schema");
      String bqTableName =
          String.format(
              "%s.%s.%s",
              pubSubToBQConfig.getOutputProjectID(),
              pubSubToBQConfig.getPubSubBQOutputDataset(),
              pubSubToBQConfig.getPubSubBQOutputTable());
      Dataset<Row> dummyDF =
          sparkSession.read().format(SPARK_READ_FORMAT_BIGQUERY).load(bqTableName);
      StructType schema = dummyDF.schema();
      LOGGER.info("Spark Schema: {}", schema);

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
              .queryName("PubSubToBQStreamingQuery")
              .foreachBatch(
                  (df, batchId) -> {
                    LOGGER.info("Processing Batch ID: {}", batchId);
                    if (!df.isEmpty()) {

                      LOGGER.info("Data is available to write for batch id: {}", batchId);
                      Dataset<Row> df_data = df.select("data");

                      LOGGER.info("Prepare BigQuery Dataframe");
                      Dataset<Row> json_df =
                          df_data
                              .withColumn("parsed_json", from_json(col("data"), schema))
                              .select("parsed_json.*");

                      LOGGER.info("Write To BigQuery With Schema: {}", json_df.schema());
                      json_df
                          .write()
                          .format(SPARK_READ_FORMAT_BIGQUERY)
                          .option("writeMethod", "direct")
                          .mode(SaveMode.Append)
                          .save(bqTableName);

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
