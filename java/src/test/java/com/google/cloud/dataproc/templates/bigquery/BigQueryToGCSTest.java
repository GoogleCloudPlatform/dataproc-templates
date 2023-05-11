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
package com.google.cloud.dataproc.templates.bigquery;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.BQ_GCS_INPUT_TABLE_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.BQ_GCS_OUTPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.BQ_GCS_OUTPUT_LOCATION;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryToGCSTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryToGCSTest.class);

  @BeforeEach
  void setup() {
    PropertyUtil.getProperties().setProperty(BQ_GCS_INPUT_TABLE_NAME, "project:dataset.table");
    PropertyUtil.getProperties().setProperty(BQ_GCS_OUTPUT_FORMAT, "avro");
    PropertyUtil.getProperties().setProperty(BQ_GCS_OUTPUT_LOCATION, "gs://test-bucket/output");
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    BigQueryToGCS template = new BigQueryToGCS();
    assertDoesNotThrow(template::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    BigQueryToGCS template = new BigQueryToGCS();
    Exception exception = assertThrows(IllegalArgumentException.class, template::validateInput);
    assertEquals(
        "Required parameters for BigQueryToGCS not passed. "
            + "Set mandatory parameter for BigQueryToGCS template in "
            + "resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(BQ_GCS_INPUT_TABLE_NAME, BQ_GCS_OUTPUT_FORMAT, BQ_GCS_OUTPUT_LOCATION);
  }
}
