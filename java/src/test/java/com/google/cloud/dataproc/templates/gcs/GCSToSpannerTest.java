/*
 * Copyright (C) 2021 Google LLC
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
import static com.google.cloud.dataproc.templates.gcs.GCSToSpannerConfig.GCS_SPANNER_INPUT_FORMAT;
import static com.google.cloud.dataproc.templates.gcs.GCSToSpannerConfig.GCS_SPANNER_INPUT_LOCATION;
import static com.google.cloud.dataproc.templates.gcs.GCSToSpannerConfig.GCS_SPANNER_OUTPUT_DATABASE;
import static com.google.cloud.dataproc.templates.gcs.GCSToSpannerConfig.GCS_SPANNER_OUTPUT_INSTANCE;
import static com.google.cloud.dataproc.templates.gcs.GCSToSpannerConfig.GCS_SPANNER_OUTPUT_PRIMARY_KEY;
import static com.google.cloud.dataproc.templates.gcs.GCSToSpannerConfig.GCS_SPANNER_OUTPUT_SAVE_MODE;
import static com.google.cloud.dataproc.templates.gcs.GCSToSpannerConfig.GCS_SPANNER_OUTPUT_TABLE;
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

class GCSToSpannerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSToSpannerTest.class);

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_INPUT_LOCATION, "some_value");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_INPUT_FORMAT, "avro");
    PropertyUtil.getProperties().setProperty(PROJECT_ID_PROP, "some_value");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_INSTANCE, "some_value");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_DATABASE, "some_value");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_TABLE, "some_value");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_SAVE_MODE, "Append");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_PRIMARY_KEY, "some_value");
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    assertDoesNotThrow((ThrowingSupplier<GCSToSpanner>) GCSToSpanner::of);
  }

  @ParameterizedTest
  @MethodSource("requiredPropertyKeys")
  void runTemplateWithMissingRequiredParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    ValidationException exception = assertThrows(ValidationException.class, GCSToSpanner::of);
  }

  static Stream<String> requiredPropertyKeys() {
    return Stream.of(
        GCS_SPANNER_INPUT_LOCATION,
        GCS_SPANNER_INPUT_FORMAT,
        PROJECT_ID_PROP,
        GCS_SPANNER_OUTPUT_INSTANCE,
        GCS_SPANNER_OUTPUT_DATABASE,
        GCS_SPANNER_OUTPUT_TABLE,
        GCS_SPANNER_OUTPUT_SAVE_MODE,
        GCS_SPANNER_OUTPUT_PRIMARY_KEY);
  }
}
