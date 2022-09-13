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

public class CassandraToGCS implements BaseTemplate, TemplateConstants {
  private String keyspace;
  private String table;
  private String cassandraHost;
  private String outputFileFormat;
  private String gcsSaveMode;
  private String gcsWritePath;
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraToGCS.class);

  public CassandraToGCS() {
    keyspace = getProperties().getProperty(CASSANDRA_TO_GSC_INPUT_KEYSPACE);
    table = getProperties().getProperty(CASSANDRA_TO_GSC_INPUT_TABLE);
    cassandraHost = getProperties().getProperty(CASSANDRA_TO_GSC_INPUT_HOST);
    outputFileFormat = getProperties().getProperty(CASSANDRA_TO_GSC_OUTPUT_FORMAT);
    gcsSaveMode = getProperties().getProperty(CASSANDRA_TO_GSC_OUTPUT_SAVE_MODE);
    gcsWritePath = getProperties().getProperty(CASSANDRA_TO_GSC_OUTPUT_PATH);
  }

  @Override
  public void runTemplate() throws StreamingQueryException, TimeoutException {
    validateInput();
    SparkSession spark =
        SparkSession.builder()
            .appName("Spark CassandraToGCS Job")
            .withExtensions(new CassandraSparkExtensions())
            .getOrCreate();
    spark
        .conf()
        .set("spark.sql.catalog.dc", "com.datastax.spark.connector.datasource.CassandraCatalog");
    spark.conf().set("spark.sql.catalog.dc.spark.cassandra.connection.host", cassandraHost);
    Dataset dataset = spark.read().table("dc." + keyspace + "." + table);
    dataset.write().format(outputFileFormat).mode(gcsSaveMode).save(gcsWritePath);
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(outputFileFormat)
        || StringUtils.isAllBlank(gcsSaveMode)
        || StringUtils.isAllBlank(gcsWritePath)
        || StringUtils.isAllBlank(cassandraHost)
        || StringUtils.isAllBlank(keyspace)
        || StringUtils.isAllBlank(table)) {
      LOGGER.error(
          "{}, {}, {}, {} is required parameter. ",
          CASSANDRA_TO_GSC_INPUT_KEYSPACE,
          CASSANDRA_TO_GSC_INPUT_HOST,
          CASSANDRA_TO_GSC_INPUT_TABLE,
          CASSANDRA_TO_GSC_OUTPUT_FORMAT,
          CASSANDRA_TO_GSC_OUTPUT_PATH,
          CASSANDRA_TO_GSC_OUTPUT_SAVE_MODE);
      throw new IllegalArgumentException(
          "Required parameters for CassandraToGCS not passed. "
              + "Set mandatory parameter for CassandraToGCS template "
              + "in resources/conf/template.properties file.");
    }
    LOGGER.info(
        "Starting Cassandra to GCS spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}"
            + "5. {},{}"
            + "6. {},{}",
        CASSANDRA_TO_GSC_INPUT_KEYSPACE,
        keyspace,
        CASSANDRA_TO_GSC_INPUT_TABLE,
        table,
        CASSANDRA_TO_GSC_INPUT_HOST,
        cassandraHost,
        CASSANDRA_TO_GSC_OUTPUT_FORMAT,
        outputFileFormat,
        CASSANDRA_TO_GSC_OUTPUT_SAVE_MODE,
        gcsSaveMode,
        CASSANDRA_TO_GSC_OUTPUT_PATH,
        gcsWritePath);
  }
}
