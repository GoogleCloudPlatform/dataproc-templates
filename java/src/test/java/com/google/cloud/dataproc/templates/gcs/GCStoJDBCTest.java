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
package com.google.cloud.dataproc.templates.gcs;

import static com.google.cloud.dataproc.templates.gcs.GCSToJDBCConfig.*;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.PROJECT_ID_PROP;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil.ValidationException;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GCStoJDBCTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCStoJDBCTest.class);

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(PROJECT_ID_PROP, "projectID");
    PropertyUtil.getProperties().setProperty(GCS_JDBC_INPUT_FORMAT, "avro");
    PropertyUtil.getProperties().setProperty(GCS_JDBC_INPUT_LOCATION, "some_value");
    PropertyUtil.getProperties().setProperty(GCS_JDBC_OUTPUT_DRIVER, "some_value");
    PropertyUtil.getProperties().setProperty(GCS_JDBC_OUTPUT_TABLE, "some_value");
    PropertyUtil.getProperties().setProperty(GCS_JDBC_OUTPUT_URL, "some_value");
    PropertyUtil.getProperties().setProperty(GCS_JDBC_OUTPUT_SAVE_MODE, "Append");
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    assertDoesNotThrow((ThrowingSupplier<GCSToJDBC>) GCSToJDBC::of);
  }

  @ParameterizedTest
  @MethodSource("requiredPropertyKeys")
  void runTemplateWithMissingRequiredParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    ValidationException exception = assertThrows(ValidationException.class, GCSToJDBC::of);
  }

  static Stream<String> requiredPropertyKeys() {
    return Stream.of(
        PROJECT_ID_PROP,
        GCS_JDBC_INPUT_FORMAT,
        GCS_JDBC_INPUT_LOCATION,
        GCS_JDBC_OUTPUT_DRIVER,
        GCS_JDBC_OUTPUT_TABLE,
        GCS_JDBC_OUTPUT_URL,
        GCS_JDBC_OUTPUT_SAVE_MODE);
  }
}
