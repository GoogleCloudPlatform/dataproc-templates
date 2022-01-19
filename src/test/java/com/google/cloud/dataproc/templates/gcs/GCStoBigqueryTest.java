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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCStoBigqueryTest {

  private GCStoBigquery gcsCsvToBiqueryTest;

  private static final Logger LOGGER = LoggerFactory.getLogger(GCStoBigqueryTest.class);

  @BeforeEach
  void setUp() {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    PropertyUtil.getProperties().setProperty(GCS_BQ_INPUT_LOCATION, "gs://test-bucket");
    PropertyUtil.getProperties().setProperty(GCS_OUTPUT_DATASET_NAME, "bigqueryDataset");
    PropertyUtil.getProperties().setProperty(GCS_OUTPUT_TABLE_NAME, "bigqueryTable");
    gcsCsvToBiqueryTest = new GCStoBigquery();

    assertDoesNotThrow(gcsCsvToBiqueryTest::runTemplate);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    gcsCsvToBiqueryTest = new GCStoBigquery();
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> gcsCsvToBiqueryTest.runTemplate());
    assertEquals(
        "Required parameters for GCStoBQ not passed. "
            + "Set mandatory parameter for GCStoBQ template"
            + " in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(GCS_BQ_INPUT_LOCATION, GCS_OUTPUT_DATASET_NAME, GCS_OUTPUT_TABLE_NAME);
  }
}
