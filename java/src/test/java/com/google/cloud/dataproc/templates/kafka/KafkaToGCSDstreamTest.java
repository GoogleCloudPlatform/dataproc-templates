/*
 * Copyright (C) 2023 Google LLC
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
import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_SCHEMA_URL;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToGCSDstreamTest {
  private KafkaToGCSDstream kafkaTOGCSDstream;
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToGCSDstreamTest.class);

  @BeforeEach
  void setup() {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    PropertyUtil.getProperties().setProperty(propKey, "some value");
    PropertyUtil.getProperties().setProperty(KAFKA_GCS_OUTPUT_LOCATION, "gs://test-bucket");
    PropertyUtil.getProperties().setProperty(KAFKA_GCS_OUTPUT_FORMAT, "bytes");
    PropertyUtil.getProperties().setProperty(KAFKA_BOOTSTRAP_SERVERS, "value");
    PropertyUtil.getProperties().setProperty(KAFKA_TOPIC, "value");
    PropertyUtil.getProperties().setProperty(KAFKA_MESSAGE_FORMAT, "csv");
    PropertyUtil.getProperties().setProperty(KAFKA_SCHEMA_URL, "");
    PropertyUtil.getProperties().setProperty(KAFKA_GCS_WRITE_MODE, "append");
    PropertyUtil.getProperties().setProperty(KAFKA_MESSAGE_FORMAT, "bytes");
    PropertyUtil.getProperties().setProperty(KAFKA_GCS_CONSUMER_GROUP_ID, "123");

    kafkaTOGCSDstream = new KafkaToGCSDstream();
    assertDoesNotThrow(kafkaTOGCSDstream::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    PropertyUtil.getProperties().setProperty(propKey, "");
    kafkaTOGCSDstream = new KafkaToGCSDstream();

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> kafkaTOGCSDstream.validateInput());
    assertEquals(
        "Required parameters for KafkaTOGCSDstream not passed. "
            + "Set mandatory parameter for KafkaTOGCSDstream template "
            + "in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        KAFKA_GCS_OUTPUT_LOCATION,
        KAFKA_GCS_OUTPUT_FORMAT,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        KAFKA_MESSAGE_FORMAT,
        KAFKA_GCS_WRITE_MODE);
  }
}
