/*
 * Copyright (C) 2025 Google LLC
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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSDeltalakeToIcebergTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSDeltalakeToIcebergTest.class);
  final Properties properties = PropertyUtil.getProperties();

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(PROJECT_ID_PROP, "projectID");
    PropertyUtil.getProperties().setProperty(DELTALAKE_INPUT_LOCATION, "gs://test-bucket/");
    PropertyUtil.getProperties()
        .setProperty(ICEBERG_TABLE_NAME, "spark_catalog.default.iceberg_table_name");

    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    GCSDeltalakeToIcebergConfig config =
        GCSDeltalakeToIcebergConfig.fromProperties(PropertyUtil.getProperties());
    GCSDeltalakeToIceberg template = new GCSDeltalakeToIceberg(config);
    assertDoesNotThrow(template::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    properties.setProperty(propKey, "");
    GCSDeltalakeToIcebergConfig config =
        GCSDeltalakeToIcebergConfig.fromProperties(PropertyUtil.getProperties());
    GCSDeltalakeToIceberg template = new GCSDeltalakeToIceberg(config);
    ValidationUtil.ValidationException exception =
        assertThrows(ValidationUtil.ValidationException.class, template::validateInput);
  }

  static Stream<String> propertyKeys() {
    return Stream.of(PROJECT_ID_PROP, DELTALAKE_INPUT_LOCATION, ICEBERG_TABLE_NAME);
  }
}
