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
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.dataproc.templates.snowflake.SnowflakeToGCS;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil.ValidationException;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraToGCSTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraToGCSTest.class);

  @BeforeEach
  void setUp() {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    PropertyUtil.getProperties()
        .setProperty(CASSANDRA_TO_GSC_OUTPUT_PATH, "gs://test-bucket/output");
    PropertyUtil.getProperties().setProperty(CASSANDRA_TO_GSC_OUTPUT_SAVE_MODE, "append");
    PropertyUtil.getProperties().setProperty(CASSANDRA_TO_GSC_OUTPUT_FORMAT, "csv");
    PropertyUtil.getProperties().setProperty(CASSANDRA_TO_GSC_INPUT_HOST, "host");
    PropertyUtil.getProperties().setProperty(CASSANDRA_TO_GSC_INPUT_TABLE, "table");
    PropertyUtil.getProperties().setProperty(CASSANDRA_TO_GSC_INPUT_KEYSPACE, "keyspace");
    assertDoesNotThrow((ThrowingSupplier<CassandraToGCS>) CassandraToGCS::of);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    ValidationException exception = assertThrows(ValidationException.class, SnowflakeToGCS::of);
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        CASSANDRA_TO_GSC_OUTPUT_PATH,
        CASSANDRA_TO_GSC_OUTPUT_SAVE_MODE,
        CASSANDRA_TO_GSC_OUTPUT_FORMAT,
        CASSANDRA_TO_GSC_INPUT_HOST,
        CASSANDRA_TO_GSC_INPUT_TABLE,
        CASSANDRA_TO_GSC_INPUT_KEYSPACE);
  }
}
