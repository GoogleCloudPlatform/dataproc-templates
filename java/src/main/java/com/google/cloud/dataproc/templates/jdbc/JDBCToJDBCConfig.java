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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_INPUT_DRIVER;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_INPUT_FETCHSIZE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_INPUT_LOWERBOUND;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_INPUT_PARTITIONCOLUMN;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_INPUT_TABLE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_INPUT_UPPERBOUND;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_INPUT_URL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_NUMPARTITIONS;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_OUTPUT_BATCH_SIZE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_OUTPUT_CREATE_TABLE_OPTION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_OUTPUT_DRIVER;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_OUTPUT_MODE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_OUTPUT_PRIMARY_KEY;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_OUTPUT_TABLE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_OUTPUT_URL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_SESSION_INIT_STATEMENT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_SQL_QUERY;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_TEMP_VIEW_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPARK_LOG_LEVEL;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.HashMap;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;

public class JDBCToJDBCConfig {
  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = JDBCTOJDBC_INPUT_URL)
  @NotEmpty
  private String jdbcInputURL;

  @JsonProperty(value = JDBCTOJDBC_INPUT_DRIVER)
  @NotEmpty
  private String jdbcInputDriver;

  @JsonProperty(value = JDBCTOJDBC_INPUT_TABLE)
  @NotEmpty
  private String jdbcInputTable;

  @JsonProperty(value = JDBCTOJDBC_INPUT_PARTITIONCOLUMN)
  private String jdbcInputPartitionColumn;

  @JsonProperty(value = JDBCTOJDBC_INPUT_LOWERBOUND)
  private String jdbcInputLowerBound;

  @JsonProperty(value = JDBCTOJDBC_INPUT_UPPERBOUND)
  private String jdbcInputUpperBound;

  @JsonProperty(value = JDBCTOJDBC_NUMPARTITIONS)
  private String jdbcNumPartitions;

  @JsonProperty(value = JDBCTOJDBC_INPUT_FETCHSIZE)
  private String jdbcInputFetchSize;

  @JsonProperty(value = JDBCTOJDBC_OUTPUT_URL)
  @NotEmpty
  private String jdbcOutputURL;

  @JsonProperty(value = JDBCTOJDBC_OUTPUT_DRIVER)
  @NotEmpty
  private String jdbcOutputDriver;

  @JsonProperty(value = JDBCTOJDBC_OUTPUT_TABLE)
  @NotEmpty
  private String jdbcOutputTable;

  @JsonProperty(value = JDBCTOJDBC_OUTPUT_CREATE_TABLE_OPTION)
  private String jdbcOutputCreateTableOption;

  @JsonProperty(value = JDBCTOJDBC_OUTPUT_MODE)
  @Pattern(regexp = "(?i)(Overwrite|ErrorIfExists|Append|Ignore)")
  private String jdbcOutputMode;

  @JsonProperty(value = JDBCTOJDBC_OUTPUT_BATCH_SIZE)
  @Min(value = 1)
  private String jdbcOutputBatchSize;

  @JsonProperty(value = JDBCTOJDBC_SESSION_INIT_STATEMENT)
  private String jdbcSessionInitStatement;

  @JsonProperty(value = JDBCTOJDBC_OUTPUT_PRIMARY_KEY)
  private String jdbcOutputPrimaryKey;

  @JsonProperty(value = JDBCTOJDBC_TEMP_VIEW_NAME)
  private String jdbcTempView;

  @JsonProperty(value = JDBCTOJDBC_SQL_QUERY)
  private String jdbcSQLQuery;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  @AssertTrue(
      message =
          "Required parameters for JDBCToJDBC not passed. "
              + "Set all the SQL partitioning properties. "
              + "Refer to jdbc/README.md for more instructions.")
  private boolean isPartitionsPropertyValid() {
    return StringUtils.isBlank(getConcatenatedPartitionProps())
        || (StringUtils.isNotBlank(jdbcInputPartitionColumn)
            && StringUtils.isNotBlank(jdbcInputLowerBound)
            && StringUtils.isNotBlank(jdbcInputUpperBound)
            && StringUtils.isNotBlank(jdbcNumPartitions));
  }

  @AssertTrue(
      message =
          "Required parameters for JDBCToJDBC not passed. "
              + "Set temp view property to do data transformations with query. "
              + "Refer to jdbc/README.md for more instructions.")
  private boolean isSqlPropertyValid() {
    return StringUtils.isBlank(jdbcSQLQuery)
        || (StringUtils.isNotBlank(jdbcSQLQuery) && StringUtils.isNotBlank(jdbcTempView));
  }

  public String getJDBCInputURL() {
    return jdbcInputURL;
  }

  public String getJDBCInputDriver() {
    return jdbcInputDriver;
  }

  public String getJDBCInputTable() {
    return jdbcInputTable;
  }

  public String getJdbcInputPartitionColumn() {
    return jdbcInputPartitionColumn;
  }

  public String getJdbcInputLowerBound() {
    return jdbcInputLowerBound;
  }

  public String getJdbcInputUpperBound() {
    return jdbcInputUpperBound;
  }

  public String getJdbcNumPartitions() {
    return jdbcNumPartitions;
  }

  public String getJdbcInputFetchSize() {
    return jdbcInputFetchSize;
  }

  public String getJdbcOutputURL() {
    return jdbcOutputURL;
  }

  public String getJdbcOutputDriver() {
    return jdbcOutputDriver;
  }

  public String getJdbcOutputTable() {
    return jdbcOutputTable;
  }

  public String getJdbcOutputCreateTableOption() {
    return jdbcOutputCreateTableOption;
  }

  public String getJdbcOutputMode() {
    return jdbcOutputMode;
  }

  public String getJdbcOutputBatchSize() {
    return jdbcOutputBatchSize;
  }

  public String getJdbcSessionInitStatement() {
    return jdbcSessionInitStatement;
  }

  public String getJdbcOutputPrimaryKey() {
    return jdbcOutputPrimaryKey;
  }

  public String getJdbcTempView() {
    return jdbcTempView;
  }

  public String getJdbcTempSQLQuery() {
    return jdbcSQLQuery;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  public String getConcatenatedPartitionProps() {
    return jdbcInputPartitionColumn + jdbcInputLowerBound + jdbcInputUpperBound + jdbcNumPartitions;
  }

  public HashMap<String, String> getJDBCProperties() {
    HashMap<String, String> jdbcProperties = new HashMap<>();

    jdbcProperties.put(JDBCOptions.JDBC_URL(), jdbcInputURL);
    jdbcProperties.put(JDBCOptions.JDBC_DRIVER_CLASS(), jdbcInputDriver);
    jdbcProperties.put(JDBCOptions.JDBC_TABLE_NAME(), jdbcInputTable);

    if (StringUtils.isNotBlank(getConcatenatedPartitionProps())) {
      jdbcProperties.put(JDBCOptions.JDBC_PARTITION_COLUMN(), jdbcInputPartitionColumn);
      jdbcProperties.put(JDBCOptions.JDBC_UPPER_BOUND(), jdbcInputUpperBound);
      jdbcProperties.put(JDBCOptions.JDBC_LOWER_BOUND(), jdbcInputLowerBound);
      jdbcProperties.put(JDBCOptions.JDBC_NUM_PARTITIONS(), jdbcNumPartitions);
    }

    if (StringUtils.isNotBlank(jdbcInputFetchSize)) {
      jdbcProperties.put(JDBCOptions.JDBC_BATCH_FETCH_SIZE(), jdbcInputFetchSize);
    }

    if (StringUtils.isNotBlank(jdbcSessionInitStatement)) {
      jdbcProperties.put(JDBCOptions.JDBC_SESSION_INIT_STATEMENT(), jdbcSessionInitStatement);
    }

    return jdbcProperties;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jdbcInputDriver", jdbcInputDriver)
        .add("jdbcInputTable", jdbcInputTable)
        .add("jdbcInputFetchSize", jdbcInputFetchSize)
        .add("jdbcInputPartitionColumn", jdbcInputPartitionColumn)
        .add("jdbcInputLowerBound", jdbcInputLowerBound)
        .add("jdbcInputUpperBound", jdbcInputUpperBound)
        .add("jdbcNumPartitions", jdbcNumPartitions)
        .add("jdbcOutputDriver", jdbcOutputDriver)
        .add("jdbcOutputTable", jdbcOutputTable)
        .add("jdbcOutputCreateTableOption", jdbcOutputCreateTableOption)
        .add("jdbcOutputMode", jdbcOutputMode)
        .add("jdbcOutputBatchSize", jdbcOutputBatchSize)
        .add("jdbcSessionInitStatement", jdbcSessionInitStatement)
        .add("jdbcOutputPrimaryKey", jdbcOutputPrimaryKey)
        .add("jdbcTempView", jdbcTempView)
        .add("jdbcSQLQuery", jdbcSQLQuery)
        .toString();
  }

  public static JDBCToJDBCConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, JDBCToJDBCConfig.class);
  }
}
