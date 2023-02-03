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
package com.google.cloud.dataproc.templates.snowflake;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
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

class SnowflakeToGCSTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeToGCSTest.class);

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(SNOWFLAKE_GCS_OUTPUT_FORMAT, "parquet");
    PropertyUtil.getProperties().setProperty(SNOWFLAKE_GCS_OUTPUT_LOCATION, "gs://some_location");
    PropertyUtil.getProperties().setProperty(SNOWFLAKE_GCS_OUTPUT_MODE, "overwrite");
    PropertyUtil.getProperties().setProperty(SNOWFLAKE_GCS_QUERY, "some_value");
    PropertyUtil.getProperties().setProperty(SNOWFLAKE_GCS_SFPASSWORD, "some_value");
    PropertyUtil.getProperties().setProperty(SNOWFLAKE_GCS_SFURL, "some_value");
    PropertyUtil.getProperties().setProperty(SNOWFLAKE_GCS_SFUSER, "some_value");
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    assertDoesNotThrow((ThrowingSupplier<SnowflakeToGCS>) SnowflakeToGCS::of);
  }

  @ParameterizedTest
  @MethodSource("requiredPropertyKeys")
  void runTemplateWithMissingRequiredParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    ValidationException exception = assertThrows(ValidationException.class, SnowflakeToGCS::of);
  }

  static Stream<String> requiredPropertyKeys() {
    return Stream.of(
        SNOWFLAKE_GCS_OUTPUT_FORMAT,
        SNOWFLAKE_GCS_OUTPUT_LOCATION,
        SNOWFLAKE_GCS_OUTPUT_MODE,
        SNOWFLAKE_GCS_QUERY,
        SNOWFLAKE_GCS_SFPASSWORD,
        SNOWFLAKE_GCS_SFURL,
        SNOWFLAKE_GCS_SFUSER);
  }
}
