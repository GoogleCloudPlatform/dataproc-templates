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
package com.google.cloud.dataproc.templates.pubsub;

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

public class PubSubToBigTableTest {

  private PubSubToBigTable pubSubToBigTableTest;
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubToBigTableTest.class);

  @BeforeEach
  void setUp() {

    System.setProperty("hadoop.home.dir", "/");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    PropertyUtil.getProperties().setProperty(PUBSUB_INPUT_PROJECT_ID_PROP, "yadavaja-sandbox");
    PropertyUtil.getProperties()
        .setProperty(PUBSUB_INPUT_SUBSCRIPTION_PROP, "pubsubtobigtable-sub");
    PropertyUtil.getProperties()
        .setProperty(PUBSUB_BIGTABLE_OUTPUT_INSTANCE_ID_PROP, "bt-templates-test");
    PropertyUtil.getProperties()
        .setProperty(PUBSUB_BIGTABLE_OUTPUT_PROJECT_ID_PROP, "yadavaja-sandbox");
    PropertyUtil.getProperties().setProperty(PUBSUB_BIGTABLE_OUTPUT_TABLE_PROP, "bus-data");

    pubSubToBigTableTest = new PubSubToBigTable();
    assertDoesNotThrow(pubSubToBigTableTest::runTemplate);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    pubSubToBigTableTest = new PubSubToBigTable();

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> pubSubToBigTableTest.runTemplate());
    assertEquals(
        "Required parameters for PubSubToBigTable not passed. "
            + "Set mandatory parameter for PubSubToBigTable template "
            + "in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        PUBSUB_INPUT_PROJECT_ID_PROP,
        PUBSUB_INPUT_SUBSCRIPTION_PROP,
        PUBSUB_BIGTABLE_OUTPUT_INSTANCE_ID_PROP,
        PUBSUB_BIGTABLE_OUTPUT_PROJECT_ID_PROP,
        PUBSUB_BIGTABLE_OUTPUT_TABLE_PROP);
  }
}
