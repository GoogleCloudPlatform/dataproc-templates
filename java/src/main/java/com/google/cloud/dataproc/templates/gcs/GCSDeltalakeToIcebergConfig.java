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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class GCSDeltalakeToIcebergConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = DELTALAKE_INPUT_LOCATION)
  @NotEmpty
  private String inputFileLocation;

  @JsonProperty(value = DELTALAKE_VERSION_AS_OF)
  @Min(value = 0)
  private int versionAsOf;

  @JsonProperty(value = DELTALAKE_TIMESTAMP_AS_OF)
  private String timestampAsOf;

  @JsonProperty(value = ICEBERG_TABLE_NAME)
  @NotEmpty
  private String icebergTableName;

  @JsonProperty(value = ICEBERG_TABLE_PARTITION_COLUMNS)
  private String icebergTablePartitionColumns;

  @JsonProperty(value = ICEBERG_GCS_OUTPUT_MODE)
  @Pattern(regexp = "(?i)(Append|Overwrite)")
  private String icebergTableWriteMode;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  public @NotEmpty String getProjectId() {
    return projectId;
  }

  public @NotEmpty String getInputFileLocation() {
    return inputFileLocation;
  }

  public int getVersionAsOf() {
    return versionAsOf;
  }

  public String getTimestampAsOf() {
    return timestampAsOf;
  }

  public @NotEmpty String getIcebergTableName() {
    return icebergTableName;
  }

  public String getIcebergTablePartitionColumns() {
    return icebergTablePartitionColumns;
  }

  public String getIcebergTableWriteMode() {
    return icebergTableWriteMode;
  }

  public @NotEmpty String getSparkLogLevel() {
    return sparkLogLevel;
  }

  @Override
  public String toString() {
    return "GCSDeltalakeToIcebergConfig{"
        + "projectId='"
        + projectId
        + '\''
        + ", inputFileLocation='"
        + inputFileLocation
        + '\''
        + ", versionAsOf="
        + versionAsOf
        + ", timestampAsOf='"
        + timestampAsOf
        + '\''
        + ", icebergTableName='"
        + icebergTableName
        + '\''
        + ", icebergTablePartitionColumns='"
        + icebergTablePartitionColumns
        + '\''
        + ", icebergTableWriteMode='"
        + icebergTableWriteMode
        + '\''
        + ", sparkLogLevel='"
        + sparkLogLevel
        + '\''
        + '}';
  }

  public static GCSDeltalakeToIcebergConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, GCSDeltalakeToIcebergConfig.class);
  }

  @AssertTrue(
      message =
          "Required parameters for GCSDeltalakeToIceberg not passed. Refer to gcs/README.md for more instructions.")
  private boolean isInputValid() {
    return StringUtils.isNotBlank(projectId)
        && StringUtils.isNotBlank(inputFileLocation)
        && StringUtils.isNotBlank(icebergTableName);
  }
}
