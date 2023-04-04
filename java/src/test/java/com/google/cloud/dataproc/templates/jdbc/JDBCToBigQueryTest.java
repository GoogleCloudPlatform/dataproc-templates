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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_BIGQUERY_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_JDBC_URL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_SQL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_TEMP_GCS_BUCKET;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBC_TO_BQ_WRITE_MODE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

public class JDBCToBigQueryTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCToBigQueryTest.class);

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_BIGQUERY_LOCATION, "project:dataset.table");
    PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_WRITE_MODE, "Append");
    PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_JDBC_URL, "url");
    PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME, "driverName");
    PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_TEMP_GCS_BUCKET, "bucket");
    PropertyUtil.getProperties().setProperty(JDBC_TO_BQ_SQL, "query");

    SparkSession spark =
        SparkSession.builder()
            .master("local")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithValidParameters");

    assertDoesNotThrow((ThrowingSupplier<JDBCToBigQuery>) JDBCToBigQuery::of);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");

    ValidationException exception = assertThrows(ValidationException.class, JDBCToBigQuery::of);
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        JDBC_TO_BQ_BIGQUERY_LOCATION,
        JDBC_TO_BQ_JDBC_URL,
        JDBC_TO_BQ_JDBC_DRIVER_CLASS_NAME,
        JDBC_TO_BQ_SQL,
        JDBC_TO_BQ_TEMP_GCS_BUCKET,
        JDBC_TO_BQ_WRITE_MODE);
  }
}
