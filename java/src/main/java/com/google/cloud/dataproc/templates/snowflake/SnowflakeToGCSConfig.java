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
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

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
  private String sfWarehouse;

  @JsonProperty(value = SNOWFLAKE_GCS_AUTOPUSHDOWN)
  @Pattern(regexp = "on|off")
  private String sfAutoPushdown;

  @JsonProperty(value = SNOWFLAKE_GCS_TABLE)
  private String sfTable;

  @JsonProperty(value = SNOWFLAKE_GCS_QUERY)
  private String sfQuery;

  @JsonProperty(value = SNOWFLAKE_GCS_OUTPUT_LOCATION)
  @NotEmpty
  @Pattern(regexp = "gs://(.+)")
  private String gcsLocation;

  @JsonProperty(value = SNOWFLAKE_GCS_OUTPUT_MODE)
  @NotEmpty
  @Pattern(regexp = "(?i)(Overwrite|ErrorIfExists|Append|Ignore)")
  private String gcsWriteMode;

  @JsonProperty(value = SNOWFLAKE_GCS_OUTPUT_FORMAT)
  @NotEmpty
  @Pattern(regexp = "(?i)(csv|avro|orc|json|parquet)")
  private String gcsWriteFormat;

  @JsonProperty(value = SNOWFLAKE_GCS_OUTPUT_PARTITION_COLUMN)
  private String gcsPartitionColumn;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

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

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  @AssertTrue(
      message =
          "Required parameters for SnowflakeToGCS not passed. Template property should be provided for either the input table or an equivalent query, but not both. Refer to snowflake/README.md for more instructions.")
  private boolean isPropertyValid() {
    return (StringUtils.isBlank(sfTable) || StringUtils.isBlank(sfQuery))
        && (StringUtils.isNotBlank(sfTable) || StringUtils.isNotBlank(sfQuery));
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

  public static SnowflakeToGCSConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, SnowflakeToGCSConfig.class);
  }
}
