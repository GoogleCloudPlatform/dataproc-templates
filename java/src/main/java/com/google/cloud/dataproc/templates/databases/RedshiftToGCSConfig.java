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
package com.google.cloud.dataproc.templates.databases;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;

public class RedshiftToGCSConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = REDSHIFT_GCS_OUTPUT_FILE_LOCATION)
  @Pattern(regexp = "gs://(.*?)/(.*)")
  @NotEmpty
  private String fileLocation;

  @JsonProperty(value = REDSHIFT_GCS_OUTPUT_FILE_FORMAT)
  @NotEmpty
  @Pattern(regexp = "csv|avro|orc|json|parquet")
  private String fileFormat;

  @JsonProperty(value = REDSHIFT_GCS_OUTPUT_MODE)
  @NotEmpty
  @Pattern(regexp = "(?i)(Overwrite|ErrorIfExists|Append|Ignore)")
  private String outputMode;

  @JsonProperty(value = REDSHIFT_AWS_INPUT_URL)
  @NotEmpty
  private String inputUrl;

  @JsonProperty(value = REDSHIFT_AWS_INPUT_TABLE)
  @NotEmpty
  private String inputTable;

  @JsonProperty(value = REDSHIFT_AWS_TEMP_DIR)
  @NotEmpty
  private String tempDir;

  @JsonProperty(value = REDSHIFT_AWS_INPUT_IAM_ROLE)
  @NotEmpty
  private String iamRole;

  @JsonProperty(value = REDSHIFT_AWS_INPUT_ACCESS_KEY)
  @NotEmpty
  private String accessKey;

  @JsonProperty(value = REDSHIFT_AWS_INPUT_SECRET_KEY)
  @NotEmpty
  private String secretKey;

  @JsonProperty(value = REDSHIFT_GCS_TEMP_TABLE)
  private String tempTable;

  @JsonProperty(value = REDSHIFT_GCS_TEMP_QUERY)
  private String tempQuery;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  public String getGcsOutputLocation() {
    return fileLocation;
  }

  public String getGcsOutputFormat() {
    return fileFormat;
  }

  public String getGcsWriteMode() {
    return outputMode;
  }

  public String getAWSURL() {
    return inputUrl;
  }

  public String getAWSTable() {
    return inputTable;
  }

  public String getAWSDir() {
    return tempDir;
  }

  public String getAWSRole() {
    return iamRole;
  }

  public String getAWSAccessKey() {
    return accessKey;
  }

  public String getAWSSecretKey() {
    return secretKey;
  }

  public String gettempTable() {
    return tempTable;
  }

  public String gettempQuery() {
    return tempQuery;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  public static com.google.cloud.dataproc.templates.databases.RedshiftToGCSConfig fromProperties(
      Properties properties) {
    return mapper.convertValue(
        properties, com.google.cloud.dataproc.templates.databases.RedshiftToGCSConfig.class);
  }
}
