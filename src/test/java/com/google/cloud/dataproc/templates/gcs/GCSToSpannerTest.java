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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_INPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_INPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_OUTPUT_DATABASE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_OUTPUT_INSTANCE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_OUTPUT_PRIMARY_KEY;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_OUTPUT_SAVE_MODE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_SPANNER_OUTPUT_TABLE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GCSToSpannerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSToSpannerTest.class);

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_INPUT_FORMAT, "avro");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_INPUT_LOCATION, "avro");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_PRIMARY_KEY, "idcol1,idcol2");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_INSTANCE, "instanceId");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_DATABASE, "databaseId");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_TABLE, "tableId");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_SAVE_MODE, "Append");
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    GCSToSpanner template = new GCSToSpanner();
    assertDoesNotThrow(template::runTemplate);
  }

  @ParameterizedTest
  @MethodSource("requiredPropertyKeys")
  void runTemplateWithMissingRequiredParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    GCSToSpanner template = new GCSToSpanner();
    Exception exception =
        assertThrows(IllegalArgumentException.class, template::runTemplate);
    assertEquals(
        "Required parameters for GCSToSpanner not passed. "
            + "Set mandatory parameter for GCSToSpanner template in "
            + "resources/conf/template.properties file.",
        exception.getMessage());
  }

  /**
   * Primary key property required if save mode is `Overwrite` or `ErrorIfExists` as spark needs try
   * know how to create the table.
   */
  @ParameterizedTest
  @MethodSource(value = "requiredPrimaryKeySaveModes")
  void runTemplateWithMissingPrimaryKey(String saveMode) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_SAVE_MODE, saveMode);
    PropertyUtil.getProperties().setProperty(GCS_SPANNER_OUTPUT_PRIMARY_KEY, "");
    GCSToSpanner template = new GCSToSpanner();
    Exception exception =
        assertThrows(IllegalArgumentException.class, template::runTemplate);
    assertEquals(
        "Parameter gcs.spanner.output.primaryKey "
            + "is required if gcs.spanner.output.saveMode is ErrorIfExists or Overwrite",
        exception.getMessage());
  }

  static Stream<String> requiredPropertyKeys() {
    return Stream.of(GCS_SPANNER_INPUT_LOCATION, GCS_SPANNER_OUTPUT_INSTANCE,
        GCS_SPANNER_OUTPUT_DATABASE, GCS_SPANNER_OUTPUT_TABLE);
  }

  static Stream<String> requiredPrimaryKeySaveModes() {
    return Stream.of(SaveMode.Overwrite.name(), SaveMode.ErrorIfExists.name());
  }
}
