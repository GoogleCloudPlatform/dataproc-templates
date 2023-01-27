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
package com.google.cloud.dataproc.templates.gcs;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_JDBC_AVRO_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_JDBC_CSV_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_JDBC_ORC_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_JDBC_PRQT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.PROJECT_ID_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPARK_LOG_LEVEL;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;
import org.apache.spark.sql.SaveMode;

public class GCSToJDBCConfig {

  public static final String GCS_JDBC_INPUT_FORMAT = "gcs.jdbc.input.format";
  public static final String GCS_JDBC_INPUT_LOCATION = "gcs.jdbc.input.location";
  public static final String GCS_JDBC_OUTPUT_URL = "gcs.jdbc.output.url";
  public static final String GCS_JDBC_OUTPUT_TABLE = "gcs.jdbc.output.table";
  public static final String GCS_JDBC_OUTPUT_SAVE_MODE = "gcs.jdbc.output.saveMode";
  public static final String GCS_JDBC_OUTPUT_BATCH_INSERT_SIZE = "gcs.jdbc.output.batchInsertSize";
  public static final String GCS_JDBC_OUTPUT_DRIVER = "gcs.jdbc.output.driver";
  public static final String CUSTOM_SPARK_PARTITIONS = "gcs.jdbc.spark.partitions";

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = GCS_JDBC_INPUT_LOCATION)
  @NotEmpty
  private String inputLocation;

  @JsonProperty(value = GCS_JDBC_INPUT_FORMAT)
  @NotEmpty
  @Pattern(
      regexp =
          GCS_JDBC_AVRO_FORMAT
              + "|"
              + GCS_JDBC_CSV_FORMAT
              + "|"
              + GCS_JDBC_ORC_FORMAT
              + "|"
              + GCS_JDBC_PRQT_FORMAT)
  private String inputFormat;

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = GCS_JDBC_OUTPUT_URL)
  @NotEmpty
  private String jdbcUrl;

  @JsonProperty(value = GCS_JDBC_OUTPUT_DRIVER)
  @NotEmpty
  private String jdbcDriver;

  @JsonProperty(value = GCS_JDBC_OUTPUT_TABLE)
  @NotEmpty
  private String table;

  @JsonProperty(value = GCS_JDBC_OUTPUT_SAVE_MODE)
  @NotEmpty
  @Pattern(regexp = "Overwrite|ErrorIfExists|Append|Ignore")
  private String saveModeString = "ErrorIfExists";

  @JsonProperty(value = GCS_JDBC_OUTPUT_BATCH_INSERT_SIZE)
  @Min(value = 1)
  private long batchInsertSize = 1000;

  @JsonProperty(value = CUSTOM_SPARK_PARTITIONS)
  @Min(value = 0)
  private String customSparkPartitions;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  public String getInputLocation() {
    return inputLocation;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getJDBCUrl() {
    return jdbcUrl;
  }

  public String getJDBCDriver() {
    return jdbcDriver;
  }

  public String getTable() {
    return table;
  }

  public String getSaveModeString() {
    return saveModeString;
  }

  @JsonIgnore
  public SaveMode getSaveMode() {
    return SaveMode.valueOf(getSaveModeString());
  }

  public long getBatchInsertSize() {
    return batchInsertSize;
  }

  public String getCustomSparkPartitions() {
    return customSparkPartitions;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("inputLocation", inputLocation)
        .add("inputFormat", inputFormat)
        .add("projectId", projectId)
        .add("batchInsertSize", batchInsertSize)
        .add("table", table)
        .add("saveModeString", saveModeString)
        .add("jdbcUrl", jdbcUrl)
        .add("jdbcDriver", jdbcDriver)
        .add("customSparkPartitions", customSparkPartitions)
        .toString();
  }

  public static GCSToJDBCConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, GCSToJDBCConfig.class);
  }
}
