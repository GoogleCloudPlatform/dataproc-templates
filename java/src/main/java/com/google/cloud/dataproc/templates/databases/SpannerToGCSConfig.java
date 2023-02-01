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
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class SpannerToGCSConfig {
  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = SPANNER_GCS_OUTPUT_GCS_PATH)
  @NotEmpty
  private String gcsOutputLocation;

  @JsonProperty(value = SPANNER_GCS_OUTPUT_FORMAT)
  @NotEmpty
  @Pattern(regexp = "avro|csv|parquet|json|orc")
  private String gcsOutputFormat;

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID)
  @NotEmpty
  private String spannerInstanceId;

  @JsonProperty(value = SPANNER_GCS_INPUT_TABLE_ID)
  @NotEmpty
  private String inputTableId;

  @JsonProperty(value = SPANNER_GCS_INPUT_DATABASE_ID)
  @NotEmpty
  private String inputDatabaseId;

  @JsonProperty(value = SPANNER_GCS_OUTPUT_GCS_SAVEMODE)
  @NotEmpty
  @Pattern(regexp = "(?i)(Append|Overwrite|ErrorIfExists|Ignore)")
  private String gcsWriteMode;

  @JsonProperty(value = SPANNER_GCS_INPUT_SQL_PARTITION_COLUMN)
  private String sqlPartitionColumn;

  @JsonProperty(value = SPANNER_GCS_INPUT_SQL_LOWER_BOUND)
  private String sqlLowerBound;

  @JsonProperty(value = SPANNER_GCS_INPUT_SQL_UPPER_BOUND)
  private String sqlUpperBound;

  @JsonProperty(value = SPANNER_GCS_INPUT_SQL_NUM_PARTITIONS)
  private String sqlNumPartitions;

  @JsonProperty(value = SPANNER_GCS_TEMP_TABLE)
  private String tempTable;

  @JsonProperty(value = SPANNER_GCS_TEMP_QUERY)
  private String tempQuery;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  @AssertTrue(
      message =
          "Optional SQL paritioning parameters are not passed correctly for SpannerToGCS. If one is specified then all needs to be specified in resources/conf/template.properties file or at runtime. Refer to databases/README.md for more instructions.")
  private boolean isPartitionsPropertyValid() {
    return StringUtils.isBlank(getConcatedPartitionProperties())
        || (StringUtils.isNotBlank(sqlPartitionColumn)
            && StringUtils.isNotBlank(sqlLowerBound)
            && StringUtils.isNotBlank(sqlUpperBound)
            && StringUtils.isNotBlank(sqlNumPartitions));
  }

  public String getGcsOutputLocation() {
    return this.gcsOutputLocation;
  }

  public String getGcsOutputFormat() {
    return this.gcsOutputFormat;
  }

  public String getProjectId() {
    return this.projectId;
  }

  public String getSpannerInstanceId() {
    return this.spannerInstanceId;
  }

  public String getInputTableId() {
    return this.inputTableId;
  }

  public String getInputDatabaseId() {
    return this.inputDatabaseId;
  }

  public String getGcsWriteMode() {
    return this.gcsWriteMode;
  }

  public String getSqlPartitionColumn() {
    return this.sqlPartitionColumn;
  }

  public String getSqlLowerBound() {
    return this.sqlLowerBound;
  }

  public String getSqlUpperBound() {
    return this.sqlUpperBound;
  }

  public String getSqlNumPartitions() {
    return this.sqlNumPartitions;
  }

  public String getTempTable() {
    return this.tempTable;
  }

  public String getTempQuery() {
    return this.tempQuery;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  public String getConcatedPartitionProperties() {
    return sqlPartitionColumn + sqlLowerBound + sqlUpperBound + sqlNumPartitions;
  }

  public String getSpannerJdbcUrl() {
    return String.format(
        "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s?lenient=true",
        projectId, spannerInstanceId, inputDatabaseId);
  }

  @Override
  public String toString() {
    return "{"
        + " gcsOutputLocation='"
        + getGcsOutputLocation()
        + "'"
        + ", gcsOutputFormat='"
        + getGcsOutputFormat()
        + "'"
        + ", projectId='"
        + getProjectId()
        + "'"
        + ", spannerInstanceId='"
        + getSpannerInstanceId()
        + "'"
        + ", inputTableId='"
        + getInputTableId()
        + "'"
        + ", inputDatabaseId='"
        + getInputDatabaseId()
        + "'"
        + ", gcsWriteMode='"
        + getGcsWriteMode()
        + "'"
        + ", sqlPartitionColumn='"
        + getSqlPartitionColumn()
        + "'"
        + ", sqlLowerBound='"
        + getSqlLowerBound()
        + "'"
        + ", sqlSQLUpperBound='"
        + getSqlUpperBound()
        + "'"
        + ", sqlNumPartitions='"
        + getSqlNumPartitions()
        + "'"
        + ", tempTable='"
        + getTempTable()
        + "'"
        + ", tempQuery='"
        + getTempQuery()
        + "'"
        + "}";
  }

  public static SpannerToGCSConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, SpannerToGCSConfig.class);
  }
}
