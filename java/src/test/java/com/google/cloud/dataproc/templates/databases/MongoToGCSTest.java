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
package com.google.cloud.dataproc.templates.databases;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_INPUT_COLLECTION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_INPUT_DATABASE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_INPUT_URI;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_OUTPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_OUTPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.MONGO_GCS_OUTPUT_MODE;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.PROJECT_ID_PROP;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoToGCSTest {

  private MongoToGCS MONGOToGCSTestObj;

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoToGCSTest.class);

  @BeforeEach
  void setUp() {
    System.setProperty("hadoop.home.dir", "/");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    Properties props = PropertyUtil.getProperties();
    PropertyUtil.getProperties().setProperty(PROJECT_ID_PROP, "projectID");
    props.setProperty(MONGO_GCS_OUTPUT_LOCATION, "gs://output-bucket");
    props.setProperty(MONGO_GCS_OUTPUT_FORMAT, "parquet");
    props.setProperty(MONGO_GCS_OUTPUT_MODE, "overwrite");
    props.setProperty(MONGO_GCS_INPUT_URI, "0.0.0.0");
    props.setProperty(MONGO_GCS_INPUT_DATABASE, "testDB");
    props.setProperty(MONGO_GCS_INPUT_COLLECTION, "testCollection");
    MONGOToGCSTestObj = new MongoToGCS();

    assertDoesNotThrow(MONGOToGCSTestObj::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    MONGOToGCSTestObj = new MongoToGCS();
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> MONGOToGCSTestObj.validateInput());
    assertEquals(
        "Required parameters for Mongo to GCS not passed. "
            + "Set mandatory parameter for Mongo to GCS template "
            + "in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        PROJECT_ID_PROP,
        MONGO_GCS_OUTPUT_LOCATION,
        MONGO_GCS_OUTPUT_FORMAT,
        MONGO_GCS_OUTPUT_MODE,
        MONGO_GCS_INPUT_URI,
        MONGO_GCS_INPUT_DATABASE,
        MONGO_GCS_INPUT_COLLECTION);
  }
}
