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
package com.google.cloud.dataproc.templates.snowflake;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotEmpty;
import java.util.Properties;

public class SnowflakeToGCSConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = SNOWFLAKE_GCS_SFURL)
  @NotEmpty
  private String sfUrl;

  @JsonProperty(value = SNOWFLAKE_GCS_SFUSER)
  @NotEmpty
  private String sfUser;

  @JsonProperty(value = SNOWFLAKE_GCS_SFPASSWORD)
  @NotEmpty
  private String sfPassword;

  @JsonProperty(value = SNOWFLAKE_GCS_SFDATABASE)
  @NotEmpty
  private String sfDatabase;

  @JsonProperty(value = SNOWFLAKE_GCS_SFSCHEMA)
  @NotEmpty
  private String sfSchema;

  @JsonProperty(value = SNOWFLAKE_GCS_SFWAREHOUSE)
  @NotEmpty
  private String sfWarehouse;

  @JsonProperty(value = SNOWFLAKE_GCS_AUTOPUSHDOWN)
  private String sfAutoPushdown;

  @JsonProperty(value = SNOWFLAKE_GCS_TABLE)
  private String sfTable;

  @JsonProperty(value = SNOWFLAKE_GCS_QUERY)
  private String sfQuery;

  @JsonProperty(value = SNOWFLAKE_GCS_OUTPUT_LOCATION)
  @NotEmpty
  private String gcsLocation;

  @JsonProperty(value = SNOWFLAKE_GCS_OUTPUT_MODE)
  @NotEmpty
  private String gcsWriteMode;

  @JsonProperty(value = SNOWFLAKE_GCS_OUTPUT_FORMAT)
  @NotEmpty
  private String gcsWriteFormat;

  @JsonProperty(value = SNOWFLAKE_GCS_OUTPUT_PARTITION_COLUMN)
  private String gcsPartitionColumn;

  public String getSfUrl() {
    return this.sfUrl;
  }

  public String getSfUser() {
    return this.sfUser;
  }

  public String getSfPassword() {
    return this.sfPassword;
  }

  public String getSfDatabase() {
    return this.sfDatabase;
  }

  public String getSfSchema() {
    return this.sfSchema;
  }

  public String getSfWarehouse() {
    return this.sfWarehouse;
  }

  public String getSfTable() {
    return this.sfTable;
  }

  public String getSfQuery() {
    return this.sfQuery;
  }

  public String getGcsLocation() {
    return this.gcsLocation;
  }

  public String getGcsWriteMode() {
    return this.gcsWriteMode;
  }

  public String getGcsWriteFormat() {
    return this.gcsWriteFormat;
  }

  public String getSfAutoPushdown() {
    return this.sfAutoPushdown;
  }

  public String getGcsPartitionColumn() {
    return this.gcsPartitionColumn;
  }

  @Override
  public String toString() {
    return "{"
        + " sfUrl='"
        + getSfUrl()
        + "'"
        + ", sfUser='"
        + getSfUser()
        + "'"
        + ", sfPassword='REDACTED'"
        + ", sfDatabase='"
        + getSfDatabase()
        + "'"
        + ", sfSchema='"
        + getSfSchema()
        + "'"
        + ", sfWarehouse='"
        + getSfWarehouse()
        + "'"
        + ", sfAutoPushdown='"
        + getSfAutoPushdown()
        + "'"
        + ", sfTable='"
        + getSfTable()
        + "'"
        + ", sfQuery='"
        + getSfQuery()
        + "'"
        + ", gcsLocation='"
        + getGcsLocation()
        + "'"
        + ", gcsWriteMode='"
        + getGcsWriteMode()
        + "'"
        + ", gcsWriteFormat='"
        + getGcsWriteFormat()
        + "'"
        + ", gcsPartitionColumn='"
        + getGcsPartitionColumn()
        + "'"
        + "}";
  }

  // TODO: Add check for only one provided query or table

  public static SnowflakeToGCSConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, SnowflakeToGCSConfig.class);
  }
}
