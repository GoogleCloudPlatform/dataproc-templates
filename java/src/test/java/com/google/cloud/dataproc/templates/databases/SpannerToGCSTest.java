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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_INPUT_DATABASE_ID;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_INPUT_TABLE_ID;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_OUTPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_OUTPUT_GCS_PATH;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.SPANNER_GCS_OUTPUT_GCS_SAVEMODE;
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

public class SpannerToGCSTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerToGCSTest.class);

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(SPANNER_GCS_OUTPUT_GCS_PATH, "gs://test-bucket/");
    PropertyUtil.getProperties().setProperty(SPANNER_GCS_OUTPUT_FORMAT, "csv");
    PropertyUtil.getProperties().setProperty(SPANNER_GCS_OUTPUT_GCS_SAVEMODE, "Append");
    PropertyUtil.getProperties().setProperty(SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID, "instanceID");
    PropertyUtil.getProperties().setProperty(SPANNER_GCS_INPUT_DATABASE_ID, "dbID");
    PropertyUtil.getProperties().setProperty(SPANNER_GCS_INPUT_TABLE_ID, "tableID");

    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithValidParameters");

    assertDoesNotThrow((ThrowingSupplier<SpannerToGCS>) SpannerToGCS::of);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");

    ValidationException exception = assertThrows(ValidationException.class, SpannerToGCS::of);
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        SPANNER_GCS_OUTPUT_GCS_PATH,
        SPANNER_GCS_OUTPUT_FORMAT,
        SPANNER_GCS_OUTPUT_GCS_SAVEMODE,
        SPANNER_GCS_INPUT_SPANNER_INSTANCE_ID,
        SPANNER_GCS_INPUT_DATABASE_ID,
        SPANNER_GCS_INPUT_TABLE_ID);
  }
}
