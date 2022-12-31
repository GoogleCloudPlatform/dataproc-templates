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
package com.google.cloud.dataproc.templates.jdbc;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;
import java.util.regex.Matcher;
import org.apache.commons.lang3.StringUtils;

public class JDBCToGCSConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = JDBC_TO_GCS_OUTPUT_LOCATION)
  @Pattern(regexp = "gs://(.*?)/(.*)")
  @NotEmpty
  private String gcsOutputLocation;

  @JsonProperty(value = JDBC_TO_GCS_OUTPUT_FORMAT)
  @NotEmpty
  @Pattern(regexp = "csv|avro|orc|json|parquet")
  private String gcsOutputFormat;

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = JDBC_TO_GCS_JDBC_URL)
  @NotEmpty
  private String jdbcURL;

  @JsonProperty(value = JDBC_TO_GCS_JDBC_DRIVER_CLASS_NAME)
  @NotEmpty
  private String jdbcDriverClassName;

  @JsonProperty(value = JDBC_TO_GCS_SQL)
  private String jdbcSQL;

  @JsonProperty(value = JDBC_TO_GCS_SQL_FILE)
  private String jdbcSQLFile;

  @JsonProperty(value = JDBC_TO_GCS_WRITE_MODE)
  @NotEmpty
  @Pattern(regexp = "(?i)(Overwrite|ErrorIfExists|Append|Ignore)")
  private String gcsWriteMode;

  @JsonProperty(value = JDBC_TO_GCS_SQL_PARTITION_COLUMN)
  private String jdbcSQLPartitionColumn;

  @JsonProperty(value = JDBC_TO_GCS_SQL_LOWER_BOUND)
  private String jdbcSQLLowerBound;

  @JsonProperty(value = JDBC_TO_GCS_SQL_UPPER_BOUND)
  private String jdbcSQLUpperBound;

  @JsonProperty(value = JDBC_TO_GCS_SQL_NUM_PARTITIONS)
  private String jdbcSQLNumPartitions;

  @JsonProperty(value = JDBC_TO_GCS_OUTPUT_PARTITION_COLUMN)
  private String gcsPartitionColumn;

  @JsonProperty(value = JDBC_TO_GCS_JDBC_FETCH_SIZE)
  private String jdbcFetchSize;

  @JsonProperty(value = JDBC_TO_GCS_TEMP_TABLE)
  private String tempTable;

  @JsonProperty(value = JDBC_TO_GCS_TEMP_QUERY)
  private String tempQuery;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  @AssertTrue(
      message =
          "Required parameters for JDBCToGCS not passed. Template property should be provided for either the SQL Query or the SQL File, but not both. Refer to jdbc/README.md for more instructions.")
  private boolean isSqlPropertyValid() {
    return (StringUtils.isBlank(jdbcSQL) || StringUtils.isBlank(jdbcSQLFile))
        && (StringUtils.isNotBlank(jdbcSQL) || StringUtils.isNotBlank(jdbcSQLFile));
  }

  @AssertTrue(
      message =
          "Required parameters for JDBCToGCS not passed. Set mandatory parameter for JDBCToGCS template in resources/conf/template.properties file or at runtime. Refer to jdbc/README.md for more instructions.")
  private boolean isPartitionsPropertyValid() {
    return StringUtils.isBlank(getConcatedPartitionProps())
        || (StringUtils.isNotBlank(jdbcSQLPartitionColumn)
            && StringUtils.isNotBlank(jdbcSQLLowerBound)
            && StringUtils.isNotBlank(jdbcSQLUpperBound)
            && StringUtils.isNotBlank(jdbcSQLNumPartitions));
  }

  public String getGcsOutputLocation() {
    return gcsOutputLocation;
  }

  public String getGcsOutputFormat() {
    return gcsOutputFormat;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getJdbcURL() {
    return jdbcURL;
  }

  public String getJdbcDriverClassName() {
    return jdbcDriverClassName;
  }

  public String getJdbcSQL() {
    return jdbcSQL;
  }

  public String getJdbcSQLFile() {
    return jdbcSQLFile;
  }

  public String getJdbcFetchSize() {
    return jdbcFetchSize;
  }

  public String getGcsWriteMode() {
    return gcsWriteMode;
  }

  public String getGcsPartitionColumn() {
    return gcsPartitionColumn;
  }

  public String getJdbcSQLLowerBound() {
    return jdbcSQLLowerBound;
  }

  public String getJdbcSQLUpperBound() {
    return jdbcSQLUpperBound;
  }

  public String getJdbcSQLNumPartitions() {
    return jdbcSQLNumPartitions;
  }

  public String getJdbcSQLPartitionColumn() {
    return jdbcSQLPartitionColumn;
  }

  public String getTempTable() {
    return tempTable;
  }

  public String getTempQuery() {
    return tempQuery;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  public String getConcatedPartitionProps() {
    return jdbcSQLPartitionColumn + jdbcSQLLowerBound + jdbcSQLUpperBound + jdbcSQLNumPartitions;
  }

  public String getSQL() {
    if (StringUtils.isNotBlank(jdbcSQL)) {
      if (jdbcURL.contains("oracle")) {
        return "(" + jdbcSQL + ")";
      } else {
        return "(" + jdbcSQL + ") as a";
      }
    } else {
      Matcher matches = java.util.regex.Pattern.compile("gs://(.*?)/(.*)").matcher(jdbcSQLFile);
      matches.matches();
      String bucket = matches.group(1);
      String objectPath = matches.group(2);

      Storage storage = StorageOptions.getDefaultInstance().getService();
      Blob blob = storage.get(bucket, objectPath);
      String fileContent = new String(blob.getContent());
      return "(" + fileContent + ") as a";
    }
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
        + ", jdbcDriverClassName='"
        + getJdbcDriverClassName()
        + "'"
        + ", jdbcSQL='"
        + getJdbcSQL()
        + "'"
        + ", jdbcSQLFile='"
        + getJdbcSQLFile()
        + "'"
        + ", gcsWriteMode='"
        + getGcsWriteMode()
        + "'"
        + ", gcsPartitionColumn='"
        + getGcsPartitionColumn()
        + "'"
        + ", jdbcSQLLowerBound='"
        + getJdbcSQLLowerBound()
        + "'"
        + ", jdbcSQLUpperBound='"
        + getJdbcSQLUpperBound()
        + "'"
        + ", jdbcSQLNumPartitions='"
        + getJdbcSQLNumPartitions()
        + "'"
        + ", jdbcSQLPartitionColumn='"
        + getJdbcSQLPartitionColumn()
        + "'"
        + ", jdbcFetchSize='"
        + getJdbcFetchSize()
        + "'"
        + "}";
  }

  public static JDBCToGCSConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, JDBCToGCSConfig.class);
  }
}
