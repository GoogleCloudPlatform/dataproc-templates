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
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class PubSubToBQTest {

  private PubSubToBQ pubSubToBQ;

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(PUBSUB_INPUT_PROJECT_ID_PROP, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUB_INPUT_SUBSCRIPTION_PROP, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUB_BQ_OUTPUT_PROJECT_ID_PROP, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUB_BQ_OUTPUT_DATASET_PROP, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUB_BQ_OUTPUT_TABLE_PROP, "some value");
    PropertyUtil.getProperties().setProperty(PUBSUB_BQ_OUTPUT_GCS_CHECKPOINT_PROP, "some value");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {

    Properties props = PropertyUtil.getProperties();
    props.setProperty(PUBSUB_INPUT_PROJECT_ID_PROP, "test-project-id");
    props.setProperty(PUBSUB_INPUT_SUBSCRIPTION_PROP, "test-subscription");
    props.setProperty(PUBSUB_BQ_OUTPUT_PROJECT_ID_PROP, "test-project-id");
    props.setProperty(PUBSUB_BQ_OUTPUT_DATASET_PROP, "test-dataset");
    props.setProperty(PUBSUB_BQ_OUTPUT_TABLE_PROP, "test-table");
    props.setProperty(PUBSUB_BQ_OUTPUT_GCS_CHECKPOINT_PROP, "test-checkpoint");

    pubSubToBQ = new PubSubToBQ(PubSubToBQConfig.fromProperties(props));
    assertDoesNotThrow(pubSubToBQ::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    PropertyUtil.getProperties().setProperty(propKey, "");
    pubSubToBQ = new PubSubToBQ(PubSubToBQConfig.fromProperties(PropertyUtil.getProperties()));

    ValidationUtil.ValidationException exception =
        assertThrows(ValidationUtil.ValidationException.class, pubSubToBQ::validateInput);
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        PUBSUB_INPUT_PROJECT_ID_PROP,
        PUBSUB_INPUT_SUBSCRIPTION_PROP,
        PUBSUB_BQ_OUTPUT_PROJECT_ID_PROP,
        PUBSUB_BQ_OUTPUT_DATASET_PROP,
        PUBSUB_BQ_OUTPUT_TABLE_PROP,
        PUBSUB_BQ_OUTPUT_GCS_CHECKPOINT_PROP);
  }
}
