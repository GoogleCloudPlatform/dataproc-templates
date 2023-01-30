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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;
import java.util.regex.Matcher;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SaveMode;

public class JDBCToSpannerConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = JDBC_TO_SPANNER_JDBC_URL)
  @NotEmpty
  private String jdbcURL;

  @JsonProperty(value = JDBC_TO_SPANNER_JDBC_DRIVER_CLASS_NAME)
  @NotEmpty
  private String jdbcDriverClassName;

  @JsonProperty(value = JDBC_TO_SPANNER_SQL)
  private String jdbcSQL;

  @JsonProperty(value = JDBC_TO_SPANNER_SQL_FILE)
  private String jdbcSQLFile;

  @JsonProperty(value = JDBC_TO_SPANNER_SQL_PARTITION_COLUMN)
  private String jdbcSQLPartitionColumn;

  @JsonProperty(value = JDBC_TO_SPANNER_SQL_LOWER_BOUND)
  private String jdbcSQLLowerBound;

  @JsonProperty(value = JDBC_TO_SPANNER_SQL_UPPER_BOUND)
  private String jdbcSQLUpperBound;

  @JsonProperty(value = JDBC_TO_SPANNER_SQL_NUM_PARTITIONS)
  private String jdbcSQLNumPartitions;

  @JsonProperty(value = JDBC_TO_SPANNER_JDBC_FETCH_SIZE)
  private String jdbcFetchSize;

  @JsonProperty(value = JDBC_TO_SPANNER_TEMP_TABLE)
  private String tempTable;

  @JsonProperty(value = JDBC_TO_SPANNER_TEMP_QUERY)
  private String tempQuery;

  @JsonProperty(value = JDBC_TO_SPANNER_OUTPUT_INSTANCE)
  @NotEmpty
  private String instance;

  @JsonProperty(value = JDBC_TO_SPANNER_OUTPUT_DATABASE)
  @NotEmpty
  private String database;

  @JsonProperty(value = JDBC_TO_SPANNER_OUTPUT_TABLE)
  @NotEmpty
  private String table;

  @JsonProperty(value = JDBC_TO_SPANNER_OUTPUT_SAVE_MODE)
  @Pattern(regexp = "Overwrite|ErrorIfExists|Append|Ignore")
  private String saveModeString = "ErrorIfExists";

  @JsonProperty(value = JDBC_TO_SPANNER_OUTPUT_PRIMARY_KEY)
  @NotEmpty
  private String primaryKey;

  @JsonProperty(value = JDBC_TO_SPANNER_OUTPUT_BATCH_INSERT_SIZE)
  @Min(value = 1)
  private long batchInsertSize = 1000;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  @AssertTrue(
      message =
          "Required parameters for JDBCToSpanner not passed. Template property should be provided for either the SQL Query or the SQL File, but not both. Refer to jdbc/README.md for more instructions.")
  private boolean isSqlPropertyValid() {
    return (StringUtils.isBlank(jdbcSQL) || StringUtils.isBlank(jdbcSQLFile))
        && (StringUtils.isNotBlank(jdbcSQL) || StringUtils.isNotBlank(jdbcSQLFile));
  }

  @AssertTrue(
      message =
          "Required parameters for JDBCToSpanner not passed. Set mandatory parameter for JDBCToSpanner template in resources/conf/template.properties file or at runtime. Refer to jdbc/README.md for more instructions.")
  private boolean isPartitionsPropertyValid() {
    return ((StringUtils.isBlank(jdbcSQLPartitionColumn)
            && StringUtils.isBlank(jdbcSQLLowerBound)
            && StringUtils.isBlank(jdbcSQLUpperBound)
            && StringUtils.isBlank(jdbcSQLNumPartitions))
        || (StringUtils.isNotBlank(jdbcSQLPartitionColumn)
            && StringUtils.isNotBlank(jdbcSQLLowerBound)
            && StringUtils.isNotBlank(jdbcSQLUpperBound)
            && StringUtils.isNotBlank(jdbcSQLNumPartitions)));
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

  public String getJdbcFetchSize() {
    return jdbcFetchSize;
  }

  public String getTempTable() {
    return tempTable;
  }

  public String getTempQuery() {
    return tempQuery;
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

  public static JDBCToSpannerConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, JDBCToSpannerConfig.class);
  }
}
