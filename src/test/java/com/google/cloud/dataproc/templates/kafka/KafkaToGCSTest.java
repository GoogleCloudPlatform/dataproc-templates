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
package com.google.cloud.dataproc.templates.kafka;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToGCSTest {

  private KafkaToGCS kafkaToGCSTest;
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToGCSTest.class);

  @BeforeEach
  void setup() {
    PropertyUtil.getProperties().setProperty(KAFKA_GCS_OUTPUT_LOCATION, "some value");
    PropertyUtil.getProperties().setProperty(KAFKA_GCS_OUTPUT_FORMAT, "some value");
    PropertyUtil.getProperties().setProperty(KAFKA_BOOTSTRAP_SERVERS, "value");
    PropertyUtil.getProperties().setProperty(KAFKA_TOPIC, "value");
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    PropertyUtil.getProperties().setProperty(propKey, "some value");

    kafkaToGCSTest = new KafkaToGCS();
    assertDoesNotThrow(kafkaToGCSTest::runTemplate);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    PropertyUtil.getProperties().setProperty(propKey, "");
    kafkaToGCSTest = new KafkaToGCS();

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> kafkaToGCSTest.runTemplate());
    assertEquals(
        "Required parameters for KafkaToGCS not passed. "
            + "Set mandatory parameter for KafkaToGCS template "
            + "in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        KAFKA_GCS_OUTPUT_LOCATION, KAFKA_GCS_OUTPUT_FORMAT, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC);
  }
}
