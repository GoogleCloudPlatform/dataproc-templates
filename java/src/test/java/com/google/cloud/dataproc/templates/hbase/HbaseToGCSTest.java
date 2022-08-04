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
package com.google.cloud.dataproc.templates.hbase;

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

public class HbaseToGCSTest {
  private HbaseToGCS hbaseToGCSTest;
  private static final Logger LOGGER = LoggerFactory.getLogger(HbaseToGCSTest.class);

  @BeforeEach
  void setUp() {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    PropertyUtil.getProperties().setProperty(HBASE_TO_GCS_OUTPUT_PATH, "gs://test-bucket/output");
    PropertyUtil.getProperties()
        .setProperty(
            HBASE_TO_GCS_TABLE_CATALOG,
            "{\"table\":{\"namespace\":\"default\",\"name\":\"my_table\"},\"rowkey\":\"key\",\"columns\":{\"key\":{\"cf\":\"rowkey\",\"col\":\"key\",\"type\":\"string\"},\"name\":{\"cf\":\"cf\",\"col\":\"name\",\"type\":\"string\"}}}");
    PropertyUtil.getProperties().setProperty(HBASE_TO_GCS_OUTPUT_SAVE_MODE, "append");
    PropertyUtil.getProperties().setProperty(HBASE_TO_GCS_OUTPUT_FILE_FORMAT, "csv");
    PropertyUtil.getProperties().setProperty(propKey, "someValue");
    hbaseToGCSTest = new HbaseToGCS();
    assertDoesNotThrow(hbaseToGCSTest::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    hbaseToGCSTest = new HbaseToGCS();

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> hbaseToGCSTest.validateInput());
    assertEquals(
        "Required parameters for HbaseToGCS not passed. "
            + "Set mandatory parameter for HbaseToGCS template in "
            + "resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        HBASE_TO_GCS_OUTPUT_FILE_FORMAT,
        HBASE_TO_GCS_OUTPUT_SAVE_MODE,
        HBASE_TO_GCS_OUTPUT_PATH,
        HBASE_TO_GCS_TABLE_CATALOG);
  }
}
