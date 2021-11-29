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
package com.google.cloud.dataproc.templates;

import com.google.cloud.dataproc.templates.options.TemplateOptionsFactory;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericTemplate implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenericTemplate.class);

  public void runTemplate() {
    Properties properties = PropertyUtil.getProperties();
    String inputFormat = properties.getProperty("input.format");
    String outputFormat = properties.getProperty("output.format");
    Map<?, ?> inputOptions =
        TemplateOptionsFactory.unscopeProperties(
            properties, String.format("input.%s.", inputFormat));
    Map<?, ?> outputOptions =
        TemplateOptionsFactory.unscopeProperties(
            properties, String.format("output.%s.", outputFormat));

    SparkSession spark = SparkSession.builder().appName("Generic Template").getOrCreate();

    String inputPath =
        Optional.ofNullable(inputOptions.remove("path")).map(Object::toString).orElse(null);
    String outputPath =
        Optional.ofNullable(outputOptions.remove("path")).map(Object::toString).orElse(null);

    LOGGER.info("Input Format: {}", inputFormat);
    LOGGER.info("Input Path: {}", inputPath);
    LOGGER.info("Input Options: {}", inputOptions);
    LOGGER.info("Output Format: {}", outputFormat);
    LOGGER.info("Output Path: {}", outputPath);
    LOGGER.info("Output Options: {}", outputOptions);

    DataFrameReader reader = spark.read().format(inputFormat);
    for (Entry<?, ?> entry : inputOptions.entrySet()) {
      reader = reader.option(entry.getKey().toString(), entry.getValue().toString());
    }
    Dataset<Row> dataset = inputPath != null ? reader.load(inputPath) : reader.load();

    DataFrameWriter<?> writer = dataset.write().format(outputFormat);
    for (Entry<?, ?> entry : inputOptions.entrySet()) {
      reader = reader.option(entry.getKey().toString(), entry.getValue().toString());
    }

    if (inputPath != null) {
      writer.save(outputPath);
    } else {
      writer.save();
    }
  }
}
