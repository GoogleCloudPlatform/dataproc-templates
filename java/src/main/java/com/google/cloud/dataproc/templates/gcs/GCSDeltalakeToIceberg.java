/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.dataproc.templates.gcs;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

public class GCSDeltalakeToIceberg implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSDeltalakeToIceberg.class);

  private final GCSDLtoIBConfig gcsdLtoIBConfig;

  public static GCSDeltalakeToIceberg of(String... args) {
    GCSDLtoIBConfig config = GCSDLtoIBConfig.fromProperties(PropertyUtil.getProperties());
    LOGGER.info("Config loaded\n{}", config);
    return new GCSDeltalakeToIceberg(config);
  }

  public GCSDeltalakeToIceberg(GCSDLtoIBConfig config) {
    this.gcsdLtoIBConfig = config;
  }

  @Override
  public void validateInput() throws IllegalArgumentException {
    ValidationUtil.validateOrThrow(gcsdLtoIBConfig);
  }

  @Override
  public void runTemplate()
      throws StreamingQueryException, TimeoutException, SQLException, InterruptedException {

    LOGGER.info("Initialize Spark Session With Iceberg And Deltalake Properties");
    SparkSession sparkSession =
        SparkSession.builder()
            .appName("GCSDeltalakeToIceberg Dataproc Job")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .getOrCreate();

    LOGGER.info("Set Log Level {}", gcsdLtoIBConfig.getSparkLogLevel());
    sparkSession.sparkContext().setLogLevel(gcsdLtoIBConfig.getSparkLogLevel());

    LOGGER.info("Read Deltalake Table From {}", gcsdLtoIBConfig.getInputFileLocation());
    String timestampAsOf =
        gcsdLtoIBConfig.getTimestampAsOf() == null || gcsdLtoIBConfig.getTimestampAsOf().isEmpty()
            ? null
            : gcsdLtoIBConfig.getTimestampAsOf();

    Dataset<Row> dataset;
    if (timestampAsOf != null) {

      LOGGER.info(
          "Time Travel By Timestamp Settings Detected With TimestampAsOf: {}", timestampAsOf);
      dataset =
          sparkSession
              .read()
              .format("delta")
              .option("timestampAsOf", timestampAsOf)
              .load(gcsdLtoIBConfig.getInputFileLocation());
    } else {

      LOGGER.info(
          "Time Travel By Version Settings Detected With VersionAsOf: {}",
          gcsdLtoIBConfig.getVersionAsOf());
      dataset =
          sparkSession
              .read()
              .format("delta")
              .option("versionAsOf", gcsdLtoIBConfig.getVersionAsOf())
              .load(gcsdLtoIBConfig.getInputFileLocation());
    }

    LOGGER.info("Write Iceberg Table To {}", gcsdLtoIBConfig.getIcebergTableName());
    String saveMode = gcsdLtoIBConfig.getIcebergTableWriteMode().toLowerCase();
    if (!gcsdLtoIBConfig.getIcebergTablePartitionColumns().isEmpty()) {
      LOGGER.info(
          "Partition Columns Detected: {}", gcsdLtoIBConfig.getIcebergTablePartitionColumns());

      dataset
          .write()
          .mode(saveMode)
          .format("iceberg")
          .partitionBy(
              JavaConverters.asScalaBuffer(gcsdLtoIBConfig.getIcebergTablePartitionColumns())
                  .toSeq())
          .saveAsTable(gcsdLtoIBConfig.getIcebergTableName());
    } else {
      dataset
          .write()
          .mode(saveMode)
          .format("iceberg")
          .saveAsTable(gcsdLtoIBConfig.getIcebergTableName());
    }

    LOGGER.info("Spark Session Stop");
    sparkSession.stop();
  }
}
