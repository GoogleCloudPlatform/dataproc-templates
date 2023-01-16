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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_GCS_INPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_GCS_INPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_GCS_OUTPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_GCS_OUTPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.GCS_GCS_WRITE_MODE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.PROJECT_ID_PROP;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCStoGCSTest {

  private GCStoGCS GCScsvToGCSTest;

  private static final Logger LOGGER = LoggerFactory.getLogger(GCStoGCSTest.class);

  @BeforeEach
  void setUp() {
    System.setProperty("hadoop.home.dir", "/");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    Properties props = PropertyUtil.getProperties();
    PropertyUtil.getProperties().setProperty(PROJECT_ID_PROP, "projectID");
    props.setProperty(GCS_GCS_INPUT_LOCATION, "gs://input-bucket");
    props.setProperty(GCS_GCS_INPUT_FORMAT, "parquet");
    props.setProperty(GCS_GCS_OUTPUT_LOCATION, "gs://output-bucket");
    props.setProperty(GCS_GCS_OUTPUT_FORMAT, "parquet");
    props.setProperty(GCS_GCS_WRITE_MODE, "overwrite");
    GCScsvToGCSTest = new GCStoGCS();

    assertDoesNotThrow(GCScsvToGCSTest::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    GCScsvToGCSTest = new GCStoGCS();
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> GCScsvToGCSTest.runTemplate());
    assertEquals(
        "Required parameters for GCStoGCS not passed. "
            + "Set mandatory parameter for GCStoGCS template"
            + " in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        PROJECT_ID_PROP,
        GCS_GCS_INPUT_LOCATION,
        GCS_GCS_INPUT_FORMAT,
        GCS_GCS_OUTPUT_LOCATION,
        GCS_GCS_OUTPUT_FORMAT,
        GCS_GCS_WRITE_MODE);
  }
}
