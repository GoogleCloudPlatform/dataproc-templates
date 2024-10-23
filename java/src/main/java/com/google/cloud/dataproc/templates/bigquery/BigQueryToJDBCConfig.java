/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.dataproc.templates.bigquery;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPARK_LOG_LEVEL;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;

public class BigQueryToJDBCConfig {

  public static final String BQ_JDBC_INPUT_TABLE_NAME = "bigquery.jdbc.input.table";
  public static final String BQ_JDBC_OUTPUT_URL = "bigquery.jdbc.url";
  public static final String BQ_JDBC_OUTPUT_BATCH_SIZE = "bigquery.jdbc.batch.size";
  public static final String BQ_JDBC_OUTPUT_DRIVER = "bigquery.jdbc.output.driver";
  public static final String BQ_JDBC_OUTPUT_TABLE_NAME = "bigquery.jdbc.output.table";
  public static final String BQ_JDBC_OUTPUT_MODE = "bigquery.jdbc.output.mode";

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = BQ_JDBC_INPUT_TABLE_NAME)
  @NotEmpty
  private String inputTableName;

  @JsonProperty(value = BQ_JDBC_OUTPUT_URL)
  @NotEmpty
  private String outputJDBCURL;

  @JsonProperty(value = BQ_JDBC_OUTPUT_BATCH_SIZE)
  @Min(value = 1)
  private long outputBatchSize;

  @JsonProperty(value = BQ_JDBC_OUTPUT_DRIVER)
  @NotEmpty
  private String outputJDBCDriver;

  @JsonProperty(value = BQ_JDBC_OUTPUT_TABLE_NAME)
  @NotEmpty
  private String outputTableName;

  @JsonProperty(value = BQ_JDBC_OUTPUT_MODE)
  @NotEmpty
  @Pattern(regexp = "Overwrite|ErrorIfExists|Append|Ignore")
  private String outputSaveMode;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparklogLevel;

  public String getInputTableName() {
    return inputTableName;
  }

  public String getOutputJDBCURL() {
    return outputJDBCURL;
  }

  public long getOutputBatchSize() {
    return outputBatchSize;
  }

  public String getOutputJDBCDriver() {
    return outputJDBCDriver;
  }

  public String getOutputTableName() {
    return outputTableName;
  }

  public String getOutputSaveMode() {
    return outputSaveMode;
  }

  public String getSparklogLevel() {
    return sparklogLevel;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("inputTableName", inputTableName)
        .add("outputJDBCURL", outputJDBCURL)
        .add("outputBatchSize", outputBatchSize)
        .add("outputJDBCDriver", outputJDBCDriver)
        .add("outputTableName", outputTableName)
        .add("outputSaveMode", outputSaveMode)
        .add("sparklogLevel", sparklogLevel)
        .toString();
  }

  public static BigQueryToJDBCConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, BigQueryToJDBCConfig.class);
  }
}
