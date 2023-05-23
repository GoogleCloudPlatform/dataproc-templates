/*
 * Copyright (C) 2023 Google LLC
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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_BQ_TEMP_QUERY;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_BQ_TEMP_TABLE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_BIGQUERY_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_JDBC_FETCH_SIZE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_JDBC_URL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_SQL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_SQL_LOWER_BOUND;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_SQL_NUM_PARTITIONS;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_SQL_PARTITION_COLUMN;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_SQL_UPPER_BOUND;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_TEMP_GCS_BUCKET;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_WRITE_MODE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPARK_LOG_LEVEL;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class JDBCToBigQueryConfig {
  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = JDBC_TO_BQ_BIGQUERY_LOCATION)
  @NotEmpty
  private String bqLocation;

  @JsonProperty(value = JDBC_TO_BQ_WRITE_MODE)
  @NotEmpty
  @Pattern(regexp = "(?i)(Append|Overwrite|ErrorIfExists|Ignore)")
  private String bqWriteMode;

  @JsonProperty(value = JDBC_TO_BQ_TEMP_GCS_BUCKET)
  @NotEmpty
  private String temporaryGcsBucket;

  @JsonProperty(value = JDBC_TO_BQ_JDBC_URL)
  @NotEmpty
  private String jdbcURL;

  @JsonProperty(value = JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME)
  @NotEmpty
  private String jdbcDriverClassName;

  @JsonProperty(value = JDBC_TO_BQ_JDBC_FETCH_SIZE)
  private String jdbcFetchSize;

  @JsonProperty(value = JDBC_TO_BQ_SQL)
  private String jdbcSQL;

  @JsonProperty(value = JDBC_TO_BQ_SQL_PARTITION_COLUMN)
  private String jdbcSQLPartitionColumn;

  @JsonProperty(value = JDBC_TO_BQ_SQL_LOWER_BOUND)
  private String jdbcSQLLowerBound;

  @JsonProperty(value = JDBC_TO_BQ_SQL_UPPER_BOUND)
  private String jdbcSQLUpperBound;

  @JsonProperty(value = JDBC_TO_BQ_SQL_NUM_PARTITIONS)
  private String jdbcSQLNumPartitions;

  @JsonProperty(value = JDBC_BQ_TEMP_TABLE)
  private String tempTable;

  @JsonProperty(value = JDBC_BQ_TEMP_QUERY)
  private String tempQuery;
  
  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  @AssertTrue(
      message =
          "Required parameters for JDBCToBigquery not passed. Set mandatory parameter for JDBCToBigQuery template in resources/conf/template.properties file or at runtime. Refer to jdbc/README.md for more instructions.")
  private boolean isSqlPropertyValid() {
    return (StringUtils.isNotBlank(bqLocation)
        && StringUtils.isNotBlank(jdbcURL)
        && StringUtils.isNotBlank(jdbcDriverClassName)
        && StringUtils.isNotBlank(jdbcSQL)
        && StringUtils.isNotBlank(temporaryGcsBucket)
        && StringUtils.isNotBlank(bqWriteMode));
  }

  @AssertTrue(
      message =
          "Required parameters for JDBCToBigquery not passed. Set all the sql partitioning parameter together for JDBCToBigquery template in resources/conf/template.properties file or at runtime. Refer to jdbc/README.md for more instructions.")
  private boolean isPartitionsPropertyValid() {

    if (StringUtils.isNotBlank(getConcatedPartitionProps())) {
      return (StringUtils.isNotBlank(jdbcSQLPartitionColumn)
              && StringUtils.isNotBlank(jdbcSQLLowerBound)
              && StringUtils.isNotBlank(jdbcSQLUpperBound))
          && StringUtils.isNotBlank(jdbcSQLNumPartitions);
    }

    return true;
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

  public String getJdbcFetchSize() {
    return jdbcFetchSize;
  }

  public String getBqLocation() {
    return bqLocation;
  }

  public String getBqWriteMode() {
    return bqWriteMode;
  }

  public String getTemporaryGcsBucket() {
    return temporaryGcsBucket;
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
    }
    return null;
  }

  @Override
  public String toString() {
    return "{"
        + " jdbcURL='"
        + getJdbcURL()
        + "'"
        + ", jdbcDriverClassName='"
        + getJdbcDriverClassName()
        + "'"
        + ", tempTable='"
        + getTempTable()
        + "'"
        + ", jdbcSQL='"
        + getJdbcSQL()
        + "'"
        + ", bqLocation='"
        + getBqLocation()
        + "'"
        + ", bqWriteMode='"
        + getBqWriteMode()
        + "'"
        + ", jdbcSQLPartitionColumn='"
        + getJdbcSQLPartitionColumn()
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
        + ", tempQuery='"
        + getTempQuery()
        + "'"
        + "}";
  }

  public static JDBCToBigQueryConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, JDBCToBigQueryConfig.class);
  }
}
