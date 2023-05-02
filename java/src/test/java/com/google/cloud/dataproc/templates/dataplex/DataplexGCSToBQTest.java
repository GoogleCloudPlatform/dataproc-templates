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
package com.google.cloud.dataproc.templates.dataplex;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.DATAPLEX_GCS_BQ_TARGET_DATASET;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.DATAPLEX_GCS_BQ_TARGET_ENTITY;
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

public class DataplexGCSToBQTest {

  private DataplexGCStoBQ dataplexGCStoBQTest;

  private static final Logger LOGGER = LoggerFactory.getLogger(DataplexGCSToBQTest.class);

  @BeforeEach
  void setUp() {
    System.setProperty("hadoop.home.dir", "/");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    Properties props = PropertyUtil.getProperties();
    props.setProperty(PROJECT_ID_PROP, "projectID");
    props.setProperty(DATAPLEX_GCS_BQ_TARGET_DATASET, "dataplex_gcs_to_bq");
    props.setProperty(
        DATAPLEX_GCS_BQ_TARGET_ENTITY,
        "projects/your-project-id/locations/your-gcp-location/lakes/your-test-lake/zones/your-zone/entities/your-entity");
    dataplexGCStoBQTest =
        new DataplexGCStoBQ(
            null,
            "projects/your-project-id/locations/your-gcp-location/lakes/your-test-lake/zones/your-zone/entities/your-entity",
            null,
            null,
            "destination_table");

    assertDoesNotThrow(dataplexGCStoBQTest::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    dataplexGCStoBQTest = new DataplexGCStoBQ(null, null, null, null, "destination_table");
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> dataplexGCStoBQTest.validateInput());
    assertEquals("Please specify the dataplexEntity property", exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        PROJECT_ID_PROP, DATAPLEX_GCS_BQ_TARGET_DATASET, DATAPLEX_GCS_BQ_TARGET_ENTITY);
  }
}
