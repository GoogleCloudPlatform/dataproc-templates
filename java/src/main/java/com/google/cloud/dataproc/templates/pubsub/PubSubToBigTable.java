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
import static org.apache.spark.sql.functions.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.pubsub.internal.GracefulStopException;
import com.google.cloud.dataproc.templates.pubsub.internal.PubSubAcker;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubToBigTable implements BaseTemplate {
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubToBigTable.class);
  private final Map<String, String> catalogColumns;

  private final PubSubToBigTableConfig pubSubToBigTableConfig;
  private volatile long lastActivityTime = System.currentTimeMillis();

  public static PubSubToBigTable of(String... args) {
    PubSubToBigTableConfig pubSubToBigTableConfig =
        PubSubToBigTableConfig.fromProperties(PropertyUtil.getProperties());
    LOGGER.info("Config loaded\n{}", pubSubToBigTableConfig);
    return new PubSubToBigTable(pubSubToBigTableConfig);
  }

  public PubSubToBigTable(PubSubToBigTableConfig pubSubToBigTableConfig) {

    this.pubSubToBigTableConfig = pubSubToBigTableConfig;
    this.catalogColumns = new HashMap<>();
  }

  @Override
  public void validateInput() throws IllegalArgumentException {
    ValidationUtil.validateOrThrow(pubSubToBigTableConfig);
  }

  @Override
  public void runTemplate() throws InterruptedException {

    LOGGER.info("Initialize Spark Session");
    SparkSession sparkSession =
        SparkSession.builder().appName("PubSubToBigTable Dataproc Job").getOrCreate();

    LOGGER.info("Set Log Level {}", pubSubToBigTableConfig.getSparkLogLevel());
    sparkSession.sparkContext().setLogLevel(pubSubToBigTableConfig.getSparkLogLevel());

    LOGGER.info("Prepare Properties");
    Map<String, String> pubsubOptions = new HashMap<>();
    pubsubOptions.put("projectId", pubSubToBigTableConfig.getInputProjectID());
    pubsubOptions.put("subscriptionId", pubSubToBigTableConfig.getPubsubInputSubscription());
    String batchSize =
        pubSubToBigTableConfig.getBatchSize() <= 0
            ? "1000"
            : String.valueOf(pubSubToBigTableConfig.getBatchSize());
    pubsubOptions.put("maxMessagesPerPull", batchSize);
    String totalReceivers =
        pubSubToBigTableConfig.getTotalReceivers() <= 0
            ? "4"
            : String.valueOf(pubSubToBigTableConfig.getTotalReceivers());
    pubsubOptions.put("numPartitions", totalReceivers);
    long timeoutMs =
        pubSubToBigTableConfig.getTimeoutMs() <= 0 ? 2000L : pubSubToBigTableConfig.getTimeoutMs();

    try {

      LOGGER.info("Retrieve BigTable Schema");
      String catalog = getBigTableCatalog();

      LOGGER.info("Retrieved Schema: {}", catalog);
      JSONObject jsonObject = new JSONObject(catalog);
      JSONObject columns = jsonObject.getJSONObject("columns");
      Iterator<String> keys = columns.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        JSONObject column = columns.getJSONObject(key);
        catalogColumns.put(key, column.getString("type"));
      }

      LOGGER.info("Available Columns and types: {}", catalogColumns);
      LOGGER.info("Create Spark Schema");
      List<StructField> fields = new ArrayList<>();
      for (String key : catalogColumns.keySet()) {

        fields.add(
            DataTypes.createStructField(key, getSparkDataType(catalogColumns.get(key)), true));
      }
      StructType schema = DataTypes.createStructType(fields);
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
              .queryName("PubSubToBigTableStreamingQuery")
              .foreachBatch(
                  (df, batchId) -> {
                    LOGGER.info("Processing Batch ID: {}", batchId);
                    if (!df.isEmpty()) {

                      LOGGER.info("Data is available to write for batch id: {}", batchId);
                      Dataset<Row> df_data = df.select("data");

                      LOGGER.info("Parse JSON Columns Data");
                      Dataset<Row> json_df =
                          df_data
                              .withColumn("parsed_json", from_json(col("data"), schema))
                              .select("parsed_json.*");

                      // This is micro batching hence we assume table is already there.
                      // BigTable throws an error if you try to re-create an existing table again.
                      LOGGER.info("Write To BigTable");
                      json_df
                          .write()
                          .format(SPARK_BIGTABLE_FORMAT)
                          .option(SPARK_BIGTABLE_CATALOG, catalog)
                          .option(
                              SPARK_BIGTABLE_PROJECT_ID,
                              pubSubToBigTableConfig.getPubSubBigTableOutputProjectId())
                          .option(
                              SPARK_BIGTABLE_INSTANCE_ID,
                              pubSubToBigTableConfig.getPubSubBigTableOutputInstanceId())
                          .option(SPARK_BIGTABLE_CREATE_NEW_TABLE, "false")
                          .save();

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
                      LOGGER.info("Throwing GracefulStopException to stop the query gracefully");
                      throw new GracefulStopException(
                          "Inactivity timeout reached, stopping stream.");
                    }
                  })
              .start();

      try {
        streamingQuery.awaitTermination();
      } catch (Exception e) {
        if (e.getCause() instanceof GracefulStopException) {
          LOGGER.warn("Inactivity timeout reached, stopping stream.");
        } else {
          LOGGER.error("Streaming Query stopped due to : ", e);
        }
      }

      LOGGER.info("Spark Session Stop");
      sparkSession.stop();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getBigTableCatalog() {
    LOGGER.info("Create Storage Object");
    Storage storage =
        StorageOptions.newBuilder()
            .setProjectId(pubSubToBigTableConfig.getInputProjectID())
            .build()
            .getService();
    Blob blob =
        storage.get(
            BlobId.fromGsUtilUri(pubSubToBigTableConfig.getPubSubBigTableCatalogLocation()));

    return new String(blob.getContent());
  }

  /**
   * Maps a BigTable data type string to its corresponding Spark {@link DataType}.
   *
   * <p>For more details, check <a
   * href="https://github.com/GoogleCloudDataproc/spark-bigtable-connector?tab=readme-ov-file#simple-data-type-serialization">
   * Spark-BigTable Connector doc</a>.
   *
   * @param bigTableDataType The source BigTable data type string (e.g., "long", "boolean").
   * @return The corresponding {@link DataType} for Spark.
   */
  private DataType getSparkDataType(String bigTableDataType) {

    switch (bigTableDataType) {
      case "boolean":
        return DataTypes.BooleanType;

      case "byte":
        return DataTypes.ByteType;

      case "short":
        return DataTypes.ShortType;

      case "int":
        return DataTypes.IntegerType;

      case "long":
        return DataTypes.LongType;

      case "float":
        return DataTypes.FloatType;

      case "double":
        return DataTypes.DoubleType;

      case "binary":
        return DataTypes.BinaryType;

        // Default to StringType for any unmatched types.
      default:
        return DataTypes.StringType;
    }
  }
}
