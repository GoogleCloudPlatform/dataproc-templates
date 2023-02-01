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

public class CassandraToBqConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = CASSANDRA_TO_BQ_INPUT_KEYSPACE)
  @NotEmpty
  private String keyspace;

  @JsonProperty(value = CASSANDRA_TO_BQ_INPUT_TABLE)
  @NotEmpty
  private String inputTable;

  @JsonProperty(value = CASSANDRA_TO_BQ_INPUT_HOST)
  @NotEmpty
  private String host;

  @JsonProperty(value = CASSANDRA_TO_BQ_BIGQUERY_LOCATION)
  @NotEmpty
  private String bqLocation;

  @JsonProperty(value = CASSANDRA_TO_BQ_WRITE_MODE)
  @NotEmpty
  @Pattern(regexp = "(?i)(Overwrite|ErrorIfExists|Append|Ignore)")
  private String mode;

  @JsonProperty(value = CASSANDRA_TO_BQ_TEMP_LOCATION)
  @NotEmpty
  private String templocation;

  @JsonProperty(value = CASSANDRA_TO_BQ_QUERY)
  private String query;

  @JsonProperty(value = CASSANDRA_TO_BQ_CATALOG)
  private String catalog = "casscon";

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  public String getKeyspace() {
    return keyspace;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getInputTable() {
    return inputTable;
  }

  public String getHost() {
    return host;
  }

  public String getQuery() {
    return query;
  }

  public String getBqLocation() {
    return bqLocation;
  }

  public String getMode() {
    return mode;
  }

  public String getTemplocation() {
    return templocation;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  @Override
  public String toString() {
    return "{"
        + " bqLocation='"
        + getBqLocation()
        + "'"
        + ", outputMode='"
        + getMode()
        + "'"
        + ", sourceKeyspace='"
        + getKeyspace()
        + "'"
        + ", sourceTable='"
        + getInputTable()
        + "'"
        + ", sourceHost='"
        + getHost()
        + "'"
        + ", tempLocation='"
        + getTemplocation()
        + "'"
        + ", catalogName='"
        + getCatalog()
        + "'"
        + ", sourceQuery='"
        + getQuery()
        + "'"
        + "}";
  }

  public static CassandraToBqConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, CassandraToBqConfig.class);
  }
}
