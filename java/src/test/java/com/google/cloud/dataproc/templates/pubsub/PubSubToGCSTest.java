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
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class PubSubToGCSTest {

  private PubSubToGCS pubSubToGCS;

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(PUBSUB_GCS_INPUT_PROJECT_ID_PROP, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUB_GCS_INPUT_SUBSCRIPTION_PROP, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUB_GCS_BUCKET_NAME, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUB_GCS_OUTPUT_DATA_FORMAT, "some value");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {

    PropertyUtil.getProperties().setProperty(propKey, "someValue");

    pubSubToGCS = new PubSubToGCS();
    assertDoesNotThrow(pubSubToGCS::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    PropertyUtil.getProperties().setProperty(propKey, "");
    pubSubToGCS = new PubSubToGCS();

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> pubSubToGCS.validateInput());
    assertEquals(
        "Required parameters for PubSubToGCS not passed. "
            + "Set mandatory parameter for PubSubToGCS template "
            + "in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        PUBSUB_GCS_INPUT_PROJECT_ID_PROP,
        PUBSUB_GCS_INPUT_SUBSCRIPTION_PROP,
        PUBSUB_GCS_BUCKET_NAME,
        PUBSUB_GCS_OUTPUT_DATA_FORMAT);
  }
}
