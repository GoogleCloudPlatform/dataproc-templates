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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveToBigQueryTest {

  private HiveToBigQuery hiveToBigQueryTest;
  private static final Logger LOGGER = LoggerFactory.getLogger(HiveToBigQueryTest.class);

  @BeforeEach
  void setUp() {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    PropertyUtil.getProperties()
        .setProperty(HIVE_TO_BQ_BIGQUERY_LOCATION, "projname.dataset.table");
    PropertyUtil.getProperties().setProperty(HIVE_TO_BQ_TEMP_GCS_BUCKET, "gs://test-bucket");
    PropertyUtil.getProperties().setProperty(HIVE_TO_BQ_SQL, "select * from default.employee");
    PropertyUtil.getProperties().setProperty(HIVE_TO_BQ_APPEND_MODE, "Append");
    PropertyUtil.getProperties().setProperty(HIVE_TO_BQ_TEMP_TABLE, "temp");
    PropertyUtil.getProperties()
        .setProperty(HIVE_TO_BQ_TEMP_QUERY, "select * from global_temp.temp");
    PropertyUtil.getProperties().setProperty(propKey, "someValue");
    hiveToBigQueryTest = new HiveToBigQuery();

    assertDoesNotThrow(hiveToBigQueryTest::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    hiveToBigQueryTest = new HiveToBigQuery();
    System.out.println("*********");
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> hiveToBigQueryTest.runTemplate());
    assertEquals(
        "Required parameters for HiveToBigQuery not passed. "
            + "Set mandatory parameter for HiveToBigQuery template in "
            + "resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        HIVE_TO_BQ_BIGQUERY_LOCATION,
        HIVE_TO_BQ_TEMP_GCS_BUCKET,
        HIVE_TO_BQ_SQL,
        HIVE_TO_BQ_APPEND_MODE,
        HIVE_TO_BQ_TEMP_TABLE,
        HIVE_TO_BQ_TEMP_QUERY);
  }
}
