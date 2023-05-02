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
package com.google.cloud.dataproc.templates.pubsublite;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.spark.bigquery.repackaged.org.json.JSONArray;
import com.google.cloud.spark.bigquery.repackaged.org.json.JSONObject;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubLiteToBigTable implements BaseTemplate, java.io.Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubLiteToBigTable.class);
  private String inputProjectID;
  private String pubsubInputSubscription;
  private String pubsubCheckpointLocation;
  private long timeoutMs;
  private int streamingDuration;
  private String pubSubBigTableOutputInstanceId;
  private String pubSubBigTableOutputProjectId;
  private String pubSubBigTableOutputTable;
  private final String sparkLogLevel;

  public PubSubLiteToBigTable() {
    inputProjectID = getProperties().getProperty(PUBSUBLITE_INPUT_PROJECT_ID_PROP);
    pubsubInputSubscription = getProperties().getProperty(PUBSUBLITE_INPUT_SUBSCRIPTION_PROP);
    timeoutMs = Long.parseLong(getProperties().getProperty(PUBSUBLITE_TIMEOUT_MS_PROP));
    streamingDuration =
        Integer.parseInt(getProperties().getProperty(PUBSUBLITE_STREAMING_DURATION_SECONDS_PROP));
    pubsubCheckpointLocation = getProperties().getProperty(PUBSUBLITE_CHECKPOINT_LOCATION_PROP);
    pubSubBigTableOutputInstanceId =
        getProperties().getProperty(PUBSUBLITE_BIGTABLE_OUTPUT_INSTANCE_ID_PROP);
    pubSubBigTableOutputProjectId =
        getProperties().getProperty(PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT_ID_PROP);
    pubSubBigTableOutputTable = getProperties().getProperty(PUBSUBLITE_BIGTABLE_OUTPUT_TABLE_PROP);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
  }

  @Override
  public void runTemplate() throws InterruptedException, TimeoutException, StreamingQueryException {

    // Initialize the Spark session
    SparkSession spark =
        SparkSession.builder().appName("Spark PubSubLiteToGCS Demo Job").getOrCreate();

    // Stream data from Pubsublite topic topic
    Dataset<Row> df =
        spark
            .readStream()
            .format(PUBSUBLITE_FORMAT)
            .option(PUBSUBLITE_SUBSCRIPTION, pubsubInputSubscription)
            .load();

    df = df.withColumn("data", df.col("data").cast(DataTypes.StringType));

    StreamingQuery query =
        df.writeStream()
            .foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>)
                    (rowDataset, aLong) -> {
                      rowDataset.foreachPartition(
                          (ForeachPartitionFunction<Row>)
                              t -> {
                                BigtableDataClient dataClient =
                                    BigtableDataClient.create(
                                        pubSubBigTableOutputProjectId,
                                        pubSubBigTableOutputInstanceId);

                                while (t.hasNext()) {
                                  long timestamp = System.currentTimeMillis() * 1000;
                                  Row row = t.next();

                                  JSONObject record = new JSONObject(row.get(4).toString());
                                  RowMutation rowMutation =
                                      RowMutation.create(
                                          pubSubBigTableOutputTable, record.getString(ROWKEY));

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
                              });
                    })
            .trigger(Trigger.ProcessingTime(streamingDuration, TimeUnit.SECONDS))
            .option(PUBSUBLITE_CHECKPOINT, pubsubCheckpointLocation)
            .start();

    // Wait enough time to execute query
    query.awaitTermination(timeoutMs); // 60s
    query.stop();

    LOGGER.info("Job completed.");
    spark.stop();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(inputProjectID)
        || StringUtils.isAllBlank(pubsubInputSubscription)
        || StringUtils.isAllBlank(pubsubCheckpointLocation)
        || StringUtils.isAllBlank(pubSubBigTableOutputInstanceId)
        || StringUtils.isAllBlank(pubSubBigTableOutputProjectId)
        || StringUtils.isAllBlank(pubSubBigTableOutputTable)) {
      LOGGER.error(
          "{},{},{},{},{} are required parameter. ",
          PUBSUBLITE_INPUT_PROJECT_ID_PROP,
          PUBSUBLITE_INPUT_SUBSCRIPTION_PROP,
          PUBSUBLITE_CHECKPOINT_LOCATION_PROP,
          PUBSUBLITE_BIGTABLE_OUTPUT_INSTANCE_ID_PROP,
          PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT_ID_PROP,
          PUBSUBLITE_BIGTABLE_OUTPUT_TABLE_PROP);
      throw new IllegalArgumentException(
          "Required parameters for PubSubLiteToBigTable not passed. "
              + "Set mandatory parameter for PubSubLiteToBigTable template "
              + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
        "Starting PubSublite to BigTable spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5. {},{}"
            + "6. {},{}"
            + "7. {},{}"
            + "8. {},{}",
        PUBSUBLITE_INPUT_PROJECT_ID_PROP,
        inputProjectID,
        PUBSUBLITE_INPUT_SUBSCRIPTION_PROP,
        pubsubInputSubscription,
        PUBSUBLITE_TIMEOUT_MS_PROP,
        timeoutMs,
        PUBSUBLITE_STREAMING_DURATION_SECONDS_PROP,
        streamingDuration,
        PUBSUBLITE_CHECKPOINT_LOCATION_PROP,
        pubsubCheckpointLocation,
        PUBSUBLITE_BIGTABLE_OUTPUT_INSTANCE_ID_PROP,
        pubSubBigTableOutputInstanceId,
        PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT_ID_PROP,
        pubSubBigTableOutputProjectId,
        PUBSUBLITE_BIGTABLE_OUTPUT_TABLE_PROP,
        pubSubBigTableOutputTable);
  }
}
