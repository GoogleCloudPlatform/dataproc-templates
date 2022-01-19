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

import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;
import org.apache.spark.sql.SaveMode;

public class GCSToSpannerConfig {

  public final static String GCS_SPANNER_INPUT_FORMAT = "gcs.spanner.input.format";
  public final static String GCS_SPANNER_INPUT_LOCATION = "gcs.spanner.input.location";
  public final static String GCS_SPANNER_OUTPUT_INSTANCE = "gcs.spanner.output.instance";
  public final static String GCS_SPANNER_OUTPUT_DATABASE = "gcs.spanner.output.database";
  public final static String GCS_SPANNER_OUTPUT_TABLE = "gcs.spanner.output.table";
  public final static String GCS_SPANNER_OUTPUT_SAVE_MODE = "gcs.spanner.output.saveMode";
  public final static String GCS_SPANNER_OUTPUT_PRIMARY_KEY = "gcs.spanner.output.primaryKey";
  public final static String GCS_SPANNER_OUTPUT_BATCH_INSERT_SIZE = "gcs.spanner.output.batchInsertSize";

  @NotEmpty
  private String inputLocation;
  @NotEmpty
  @Pattern(regexp = "avro|parquet")
  private String inputFormat;
  @NotEmpty
  private String projectId;
  @NotEmpty
  private String instance;
  @NotEmpty
  private String database;
  @NotEmpty
  private String table;
  @Pattern(regexp = "Overwrite|ErrorIfExists|Append|Ignore")
  private String saveModeString = "ErrorIfExists";
  @NotEmpty
  private String primaryKey;
  @Min(value = 1)
  private long batchInsertSize = 1000;

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

  public SaveMode getSaveMode() {
    return SaveMode.valueOf(getSaveModeString());
  }

  public String getPrimaryKey() {
    return primaryKey;
  }

  public long getBatchInsertSize() {
    return batchInsertSize;
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
    GCSToSpannerConfig config = new GCSToSpannerConfig();
    config.inputFormat = properties.getProperty(GCS_SPANNER_INPUT_FORMAT);
    config.inputLocation = properties.getProperty(GCS_SPANNER_INPUT_LOCATION);
    config.projectId = properties.getProperty(PROJECT_ID_PROP);
    config.instance = properties.getProperty(GCS_SPANNER_OUTPUT_INSTANCE);
    config.database = properties.getProperty(GCS_SPANNER_OUTPUT_DATABASE);
    config.table = properties.getProperty(GCS_SPANNER_OUTPUT_TABLE);
    config.saveModeString = properties
        .getProperty(GCS_SPANNER_OUTPUT_SAVE_MODE, SaveMode.ErrorIfExists.toString());
    config.primaryKey = properties.getProperty(GCS_SPANNER_OUTPUT_PRIMARY_KEY);
    if (!Strings.isNullOrEmpty(properties.getProperty(GCS_SPANNER_OUTPUT_BATCH_INSERT_SIZE))) {
      config.batchInsertSize = Long.parseLong(
          properties.getProperty(GCS_SPANNER_OUTPUT_BATCH_INSERT_SIZE));
    }
    return config;
  }
}
