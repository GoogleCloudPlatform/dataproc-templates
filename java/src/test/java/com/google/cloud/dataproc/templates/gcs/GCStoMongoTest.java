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
package com.google.cloud.dataproc.templates.gcs;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.*;

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

public class GCStoMongoTest {
  private GCStoMongo gcstoMongoTestObject;
  private static final Logger LOGGER = LoggerFactory.getLogger(GCStoMongoTest.class);

  @BeforeEach
  void setUp() {
    System.setProperty("hadoop.home.dir", "/");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    Properties props = PropertyUtil.getProperties();
    PropertyUtil.getProperties().setProperty(GCS_MONGO_INPUT_LOCATION, "gs://test-bucket");
    PropertyUtil.getProperties().setProperty(GCS_MONGO_INPUT_FORMAT, "csv");
    PropertyUtil.getProperties().setProperty(GCS_MONGO_URL, "mongodb://1.2.3.4:27017");
    PropertyUtil.getProperties().setProperty(GCS_MONGO_DATABASE, "database");
    PropertyUtil.getProperties().setProperty(GCS_MONGO_COLLECTION, "collection");
    gcstoMongoTestObject = new GCStoMongo();
    assertDoesNotThrow(gcstoMongoTestObject::validateInput);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    gcstoMongoTestObject = new GCStoMongo();
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> gcstoMongoTestObject.validateInput());
    assertEquals(
        "Required parameters for GCStoMongo not passed. "
            + "Set mandatory parameter for GCStoMongo template"
            + " in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        GCS_MONGO_INPUT_LOCATION,
        GCS_MONGO_INPUT_FORMAT,
        GCS_MONGO_URL,
        GCS_MONGO_DATABASE,
        GCS_MONGO_COLLECTION);
  }
}
