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
package com.google.cloud.dataproc.templates.pubsublite;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubLiteToGCS implements BaseTemplate {
  public static final Logger LOGGER = LoggerFactory.getLogger(PubSubLiteToGCS.class);

  private final String inputSubscriptionUrl;
  private final String checkpointLocation;
  private final String timeoutMs;
  private final String processingTime;
  private final String outputBucket;
  private final String writeMode;
  private final String outputFormat;
  private final String sparkLogLevel;

  public PubSubLiteToGCS() {
    inputSubscriptionUrl = getProperties().getProperty(PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL);
    checkpointLocation = getProperties().getProperty(PUBSUBLITE_CHECKPOINT_LOCATION);
    timeoutMs = getProperties().getProperty(PUBSUBLITE_TO_GCS_TIMEOUT_MS);
    processingTime = getProperties().getProperty(PUBSUBLITE_TO_GCS_PROCESSING_TIME_SECONDS);
    outputBucket = getProperties().getProperty(PUBSUBLITE_TO_GCS_OUTPUT_LOCATION);
    writeMode = getProperties().getProperty(PUBSUBLITE_TO_GCS_WRITE_MODE);
    outputFormat = getProperties().getProperty(PUBSUBLITE_TO_GCS_OUTPUT_FORMAT);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() throws TimeoutException, StreamingQueryException {
    validateInput();

    // Initialize Spark Session

    SparkSession spark = null;
    spark = SparkSession.builder().appName("PubSubLite To GCS").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);
    Dataset<Row> df =
        spark
            .readStream()
            .format("pubsublite")
            .option("pubsublite.subscription", inputSubscriptionUrl)
            .load();

    StreamingQuery query =
        df.writeStream()
            .format(outputFormat)
            .option("checkpointLocation", checkpointLocation)
            .option("path", outputBucket)
            .outputMode(writeMode)
            .trigger(Trigger.ProcessingTime(Integer.parseInt(processingTime), TimeUnit.SECONDS))
            .start();

    // Wait enough time to execute query
    query.awaitTermination(Integer.parseInt(timeoutMs));
    query.stop();

    LOGGER.info("Job completed.");
    spark.stop();
  }

  @Override
  public void validateInput() {}
}
