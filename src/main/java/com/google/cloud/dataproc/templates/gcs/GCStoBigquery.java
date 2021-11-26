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
package com.google.cloud.dataproc.templates.gcs;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.options.BigQueryOutputOptions;
import com.google.cloud.dataproc.templates.options.GCSInputOptions;
import com.google.cloud.dataproc.templates.options.TemplateOptions;
import com.google.cloud.dataproc.templates.options.TemplateOptionsFactory;
import java.util.Objects;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCStoBigquery implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(GCStoBigquery.class);

  @Override
  public void runTemplate() {
    Properties props = getProperties();
    TemplateOptionsFactory<TemplateOptions> optionsFactory =
        TemplateOptionsFactory.fromProps(props);
    TemplateOptions options = optionsFactory.create();
    GCSInputOptions inputOptions = optionsFactory.as(GCSInputOptions.class).create();
    BigQueryOutputOptions outputOptions = optionsFactory.as(BigQueryOutputOptions.class).create();
    LOGGER.info("{}", options);
    LOGGER.info("{}", inputOptions);
    LOGGER.info("{}", outputOptions);

    SparkSession spark = null;
    try {
      spark = SparkSession.builder().appName("GCS to Bigquery load").getOrCreate();

      Dataset<Row> inputData = null;

      switch (inputOptions.getFormatEnum()) {
        case CSV:
          inputData =
              spark
                  .read()
                  .format("csv")
                  .option("header", true)
                  .option("inferSchema", true)
                  .load(inputOptions.getPath());
          break;
        case AVRO:
          inputData = spark.read().format("com.databricks.spark.avro").load(inputOptions.getPath());
          break;
        case PARQUET:
          inputData = spark.read().parquet(inputOptions.getPath());
          break;
        case JSON:
          inputData = spark.read().json(inputOptions.getPath());
          break;
        default:
          throw new IllegalArgumentException("Format not supported");
      }

      inputData
          .write()
          .format("com.google.cloud.spark.bigquery")
          .option("table", outputOptions.getTable())
          .option("temporaryGcsBucket", outputOptions.getTemporaryGcsBucket())
          .mode(SaveMode.Append)
          .save();

    } catch (Throwable th) {
      LOGGER.error("Exception in GCStoBigquery", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
