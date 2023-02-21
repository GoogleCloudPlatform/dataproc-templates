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
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCStoBigqueryTest {

  private GCStoBigquery gcsCsvToBiqueryTest;

  private static final Logger LOGGER = LoggerFactory.getLogger(GCStoBigqueryTest.class);

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
    props.setProperty(GCS_BQ_INPUT_LOCATION, "gs://test-bucket");
    props.setProperty(GCS_OUTPUT_DATASET_NAME, "bigqueryDataset");
    props.setProperty(GCS_OUTPUT_TABLE_NAME, "bigqueryTable");
    props.setProperty(GCS_BQ_INPUT_FORMAT, "parquet");
    props.setProperty(GCS_BQ_LD_TEMP_BUCKET_NAME, "gs://temp-bucket");
    gcsCsvToBiqueryTest = new GCStoBigquery();

    assertDoesNotThrow(gcsCsvToBiqueryTest::validateInput);
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
    return Stream.of(
        PROJECT_ID_PROP,
        GCS_BQ_INPUT_LOCATION,
        GCS_OUTPUT_DATASET_NAME,
        GCS_OUTPUT_TABLE_NAME,
        GCS_BQ_INPUT_FORMAT);
  }
}
