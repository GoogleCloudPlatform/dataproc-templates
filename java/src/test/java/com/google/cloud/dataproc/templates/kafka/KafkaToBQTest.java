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
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class KafkaToBQTest {

  private KafkaToBQ kafkaToBQ;

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(PROJECT_ID_PROP, "projectID");
    PropertyUtil.getProperties().setProperty(KAFKA_BQ_BOOTSTRAP_SERVERS, "some_value");
    PropertyUtil.getProperties().setProperty(KAFKA_BQ_TOPIC, "some_value");
    PropertyUtil.getProperties().setProperty(KAFKA_BQ_CHECKPOINT_LOCATION, "some_value");
    PropertyUtil.getProperties().setProperty(KAFKA_BQ_DATASET, "some_value");
    PropertyUtil.getProperties().setProperty(KAFKA_BQ_TABLE, "some_value");
    PropertyUtil.getProperties().setProperty(KAFKA_BQ_TEMP_GCS_BUCKET, "some_value");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {

    PropertyUtil.getProperties().setProperty(propKey, "someValue");

    kafkaToBQ = new KafkaToBQ();
    assertDoesNotThrow(kafkaToBQ::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    PropertyUtil.getProperties().setProperty(propKey, "");
    kafkaToBQ = new KafkaToBQ();

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> kafkaToBQ.runTemplate());
    assertEquals(
        "Required parameters for KafkaToBQ not passed. "
            + "Set mandatory parameter for KafkaToBQ template "
            + "in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        PROJECT_ID_PROP,
        KAFKA_BQ_BOOTSTRAP_SERVERS,
        KAFKA_BQ_TOPIC,
        KAFKA_BQ_CHECKPOINT_LOCATION,
        KAFKA_BQ_DATASET,
        KAFKA_BQ_TABLE,
        KAFKA_BQ_TEMP_GCS_BUCKET);
  }
}
