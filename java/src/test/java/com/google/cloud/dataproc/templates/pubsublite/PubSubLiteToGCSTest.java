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
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class PubSubLiteToGCSTest {

  private PubSubLiteToGCS pubSubLiteToGCS;

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties()
        .setProperty(PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUBLITE_TO_GCS_TIMEOUT_MS, "some value");
    PropertyUtil.getProperties()
        .setProperty(PUBSUBLITE_TO_GCS_PROCESSING_TIME_SECONDS, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUBLITE_TO_GCS_OUTPUT_LOCATION, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUBLITE_CHECKPOINT_LOCATION, "some value");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {

    PropertyUtil.getProperties().setProperty(propKey, "someValue");

    pubSubLiteToGCS = new PubSubLiteToGCS();
    assertDoesNotThrow(pubSubLiteToGCS::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    PropertyUtil.getProperties().setProperty(propKey, "");
    pubSubLiteToGCS = new PubSubLiteToGCS();

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> pubSubLiteToGCS.runTemplate());
    assertEquals(
        "Required parameters for PubSubLiteToGCS not passed. "
            + "Set mandatory parameter for PubSubLiteToGCS template "
            + "in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL,
        PUBSUBLITE_TO_GCS_TIMEOUT_MS,
        PUBSUBLITE_TO_GCS_PROCESSING_TIME_SECONDS,
        PUBSUBLITE_TO_GCS_OUTPUT_LOCATION,
        PUBSUBLITE_CHECKPOINT_LOCATION);
  }
}
