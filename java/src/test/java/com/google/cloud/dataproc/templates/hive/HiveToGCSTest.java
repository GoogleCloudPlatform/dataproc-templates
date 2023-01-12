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
package com.google.cloud.dataproc.templates.hive;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_GCS_TEMP_QUERY;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_GCS_TEMP_TABLE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_INPUT_TABLE_DATABASE_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_INPUT_TABLE_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.HIVE_TO_GCS_OUTPUT_PATH_PROP;
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

class HiveToGCSTest {

  private HiveToGCS hiveToGCSTest;
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveToGCSTest.class);

  @BeforeEach
  void setUp() {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    PropertyUtil.getProperties().setProperty(HIVE_INPUT_TABLE_DATABASE_PROP, "dbName");
    PropertyUtil.getProperties().setProperty(HIVE_TO_GCS_OUTPUT_PATH_PROP, "gs://test-bucket");
    PropertyUtil.getProperties().setProperty(HIVE_INPUT_TABLE_PROP, "tableName");
    PropertyUtil.getProperties().setProperty(HIVE_GCS_TEMP_TABLE, "demo");
    PropertyUtil.getProperties().setProperty(HIVE_GCS_TEMP_QUERY, "select * from global_temp.demo");
    hiveToGCSTest = new HiveToGCS();

    assertDoesNotThrow(hiveToGCSTest::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    hiveToGCSTest = new HiveToGCS();

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> hiveToGCSTest.runTemplate());
    assertEquals(
        "Required parameters for HiveToGCS not passed. "
            + "Set mandatory parameter for HiveToGCS template in "
            + "resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        HIVE_INPUT_TABLE_DATABASE_PROP,
        HIVE_TO_GCS_OUTPUT_PATH_PROP,
        HIVE_INPUT_TABLE_PROP,
        HIVE_GCS_TEMP_TABLE,
        HIVE_GCS_TEMP_QUERY);
  }
}
