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
package com.google.cloud.dataproc.templates.pubsublite;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubLiteToBigTableTest {

  private PubSubLiteToBigTable pubSubLiteToBigTableTest;
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubLiteToBigTableTest.class);

  @BeforeEach
  void setUp() {

    System.setProperty("hadoop.home.dir", "/");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    PropertyUtil.getProperties().setProperty(PUBSUBLITE_INPUT_PROJECT_ID_PROP, "some-value");
    PropertyUtil.getProperties().setProperty(PUBSUBLITE_INPUT_SUBSCRIPTION_PROP, "some-value");
    PropertyUtil.getProperties().setProperty(PUBSUBLITE_CHECKPOINT_LOCATION_PROP, "some-value");
    PropertyUtil.getProperties()
        .setProperty(PUBSUBLITE_BIGTABLE_OUTPUT_INSTANCE_ID_PROP, "some-value");
    PropertyUtil.getProperties()
        .setProperty(PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT_ID_PROP, "some-value");
    PropertyUtil.getProperties().setProperty(PUBSUBLITE_BIGTABLE_OUTPUT_TABLE_PROP, "some-value");

    pubSubLiteToBigTableTest = new PubSubLiteToBigTable();
    assertDoesNotThrow(pubSubLiteToBigTableTest::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    pubSubLiteToBigTableTest = new PubSubLiteToBigTable();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> pubSubLiteToBigTableTest.validateInput());
    assertEquals(
        "Required parameters for PubSubLiteToBigTable not passed. "
            + "Set mandatory parameter for PubSubLiteToBigTable template "
            + "in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        PUBSUBLITE_INPUT_PROJECT_ID_PROP,
        PUBSUBLITE_INPUT_SUBSCRIPTION_PROP,
        PUBSUBLITE_CHECKPOINT_LOCATION_PROP,
        PUBSUBLITE_BIGTABLE_OUTPUT_INSTANCE_ID_PROP,
        PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT_ID_PROP,
        PUBSUBLITE_BIGTABLE_OUTPUT_TABLE_PROP);
  }
}
