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

public class GCSToSpannerConfig {

  public static final String GCS_SPANNER_INPUT_FORMAT = "gcs.spanner.input.format";
  public static final String GCS_SPANNER_INPUT_LOCATION = "gcs.spanner.input.location";
  public static final String GCS_SPANNER_OUTPUT_INSTANCE = "gcs.spanner.output.instance";
  public static final String GCS_SPANNER_OUTPUT_DATABASE = "gcs.spanner.output.database";
  public static final String GCS_SPANNER_OUTPUT_TABLE = "gcs.spanner.output.table";
  public static final String GCS_SPANNER_OUTPUT_SAVE_MODE = "gcs.spanner.output.saveMode";
  public static final String GCS_SPANNER_OUTPUT_PRIMARY_KEY = "gcs.spanner.output.primaryKey";
  public static final String GCS_SPANNER_OUTPUT_BATCH_INSERT_SIZE =
      "gcs.spanner.output.batchInsertSize";
  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = GCS_SPANNER_INPUT_LOCATION)
  @NotEmpty
  private String inputLocation;

  @JsonProperty(value = GCS_SPANNER_INPUT_FORMAT)
  @NotEmpty
  @Pattern(regexp = "avro|parquet|orc")
  private String inputFormat;

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = GCS_SPANNER_OUTPUT_INSTANCE)
  @NotEmpty
  private String instance;

  @JsonProperty(value = GCS_SPANNER_OUTPUT_DATABASE)
  @NotEmpty
  private String database;

  @JsonProperty(value = GCS_SPANNER_OUTPUT_TABLE)
  @NotEmpty
  private String table;

  @JsonProperty(value = GCS_SPANNER_OUTPUT_SAVE_MODE)
  @Pattern(regexp = "Overwrite|ErrorIfExists|Append|Ignore")
  private String saveModeString = "ErrorIfExists";

  @JsonProperty(value = GCS_SPANNER_OUTPUT_PRIMARY_KEY)
  @NotEmpty
  private String primaryKey;

  @JsonProperty(value = GCS_SPANNER_OUTPUT_BATCH_INSERT_SIZE)
  @Min(value = 1)
  private long batchInsertSize = 1000;

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

  public String getInstance() {
    return instance;
  }

  public String getDatabase() {
    return database;
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

  public String getPrimaryKey() {
    return primaryKey;
  }

  public long getBatchInsertSize() {
    return batchInsertSize;
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
        .add("instance", instance)
        .add("database", database)
        .add("table", table)
        .add("saveModeString", saveModeString)
        .add("primaryKey", primaryKey)
        .add("batchInsertSize", batchInsertSize)
        .toString();
  }

  public static GCSToSpannerConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, GCSToSpannerConfig.class);
  }
}
