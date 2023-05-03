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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_INPUT_DRIVER;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_INPUT_TABLE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_INPUT_URL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_OUTPUT_DRIVER;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_OUTPUT_TABLE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.JDBCTOJDBC_OUTPUT_URL;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil.ValidationException;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JDBCToJDBCTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCToJDBCTest.class);

  final Properties properties = PropertyUtil.getProperties();

  @BeforeEach
  void setUp() {
    properties.setProperty(JDBCTOJDBC_INPUT_URL, "input_url");
    properties.setProperty(JDBCTOJDBC_INPUT_DRIVER, "input_driver");
    properties.setProperty(JDBCTOJDBC_INPUT_TABLE, "input_table");
    properties.setProperty(JDBCTOJDBC_OUTPUT_URL, "output_url");
    properties.setProperty(JDBCTOJDBC_OUTPUT_DRIVER, "output_driver");
    properties.setProperty(JDBCTOJDBC_OUTPUT_TABLE, "output_table");
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    JDBCToJDBCConfig config = JDBCToJDBCConfig.fromProperties(PropertyUtil.getProperties());
    JDBCToJDBC template = new JDBCToJDBC(config);
    assertDoesNotThrow(template::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    properties.setProperty(propKey, "");
    JDBCToJDBCConfig config = JDBCToJDBCConfig.fromProperties(PropertyUtil.getProperties());
    JDBCToJDBC template = new JDBCToJDBC(config);
    ValidationException exception =
        assertThrows(ValidationException.class, template::validateInput);
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        JDBCTOJDBC_INPUT_URL,
        JDBCTOJDBC_INPUT_DRIVER,
        JDBCTOJDBC_INPUT_TABLE,
        JDBCTOJDBC_OUTPUT_URL,
        JDBCTOJDBC_OUTPUT_DRIVER,
        JDBCTOJDBC_OUTPUT_TABLE);
  }
}
