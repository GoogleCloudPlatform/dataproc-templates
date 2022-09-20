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

import com.datastax.spark.connector.CassandraSparkExtensions;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.TemplateConstants;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraToBQ implements BaseTemplate, TemplateConstants {
  private String keyspace;
  private String table;
  private String cassandraHost;
  private String bqLocation;
  private String bqWriteMode;
  private String tempLocation;
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraToBQ.class);

  public CassandraToBQ() {
    keyspace = getProperties().getProperty(CASSANDRA_TO_BQ_INPUT_KEYSPACE);
    table = getProperties().getProperty(CASSANDRA_TO_BQ_INPUT_TABLE);
    cassandraHost = getProperties().getProperty(CASSANDRA_TO_BQ_INPUT_HOST);
    bqLocation = getProperties().getProperty(CASSANDRA_TO_BQ_BIGQUERY_LOCATION);
    bqWriteMode = getProperties().getProperty(CASSANDRA_TO_BQ_WRITE_MODE);
    tempLocation= getProperties().getProperty(CASSANDRA_TO_BQ_TEMP_LOCATION);
  }

  @Override
  public void runTemplate() throws StreamingQueryException, TimeoutException {
    validateInput();
    SparkSession spark =
        SparkSession.builder()
            .appName("Spark CassandraToGCS Job")
            .withExtensions(new CassandraSparkExtensions())
            .config(
                "spark.sql.catalog.dc", "com.datastax.spark.connector.datasource.CassandraCatalog")
            .config("spark.sql.catalog.dc.spark.cassandra.connection.host", cassandraHost)
            .getOrCreate();
    Dataset dataset = spark.sql("SELECT * FROM dc" + "." + keyspace + "." + table);
    dataset
        .write()
        .mode(bqWriteMode)
        .format("com.google.cloud.spark.bigquery")
        .option("table", bqLocation)
        .save();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(bqLocation)
        || StringUtils.isAllBlank(bqWriteMode)
        || StringUtils.isAllBlank(cassandraHost)
        || StringUtils.isAllBlank(keyspace)
        || StringUtils.isAllBlank(table)) {
      LOGGER.error(
          "{}, {}, {}, {}, {} is required parameter. ",
          CASSANDRA_TO_BQ_BIGQUERY_LOCATION,
          CASSANDRA_TO_BQ_WRITE_MODE,
          CASSANDRA_TO_BQ_INPUT_HOST,
          CASSANDRA_TO_BQ_INPUT_KEYSPACE,
          CASSANDRA_TO_BQ_INPUT_TABLE);
      throw new IllegalArgumentException(
          "Required parameters for CassandraToBQ not passed. "
              + "Set mandatory parameter for CassandraToBQ template "
              + "in resources/conf/template.properties file.");
    }
    LOGGER.info(
        "Starting Cassandra to BQ spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5. {},{}",
        CASSANDRA_TO_BQ_INPUT_KEYSPACE,
        keyspace,
        CASSANDRA_TO_BQ_INPUT_TABLE,
        table,
        CASSANDRA_TO_BQ_INPUT_HOST,
        cassandraHost,
        CASSANDRA_TO_BQ_BIGQUERY_LOCATION,
        bqLocation,
        CASSANDRA_TO_BQ_WRITE_MODE,
        bqWriteMode);
  }
}
