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
package com.google.cloud.dataproc.templates.s3;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.PROJECT_ID_PROP;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.S3_BQ_ACCESS_KEY;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.S3_BQ_INPUT_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.S3_BQ_INPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.S3_BQ_LD_TEMP_BUCKET_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.S3_BQ_OUTPUT_DATASET_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.S3_BQ_OUTPUT_TABLE_NAME;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.S3_BQ_SECRET_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ToBigQueryTest {
  private S3ToBigQuery s3ToBigQueryTest;
  private static final Logger LOGGER = LoggerFactory.getLogger(S3ToBigQueryTest.class);

  @BeforeEach
  void setUp() {
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithValidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    PropertyUtil.getProperties().setProperty(PROJECT_ID_PROP, "projectID");
    PropertyUtil.getProperties().setProperty(S3_BQ_INPUT_LOCATION, "s3a://test-bucket");
    PropertyUtil.getProperties().setProperty(S3_BQ_OUTPUT_DATASET_NAME, "bigqueryDataset");
    PropertyUtil.getProperties().setProperty(S3_BQ_OUTPUT_TABLE_NAME, "bigqueryTable");
    PropertyUtil.getProperties().setProperty(S3_BQ_ACCESS_KEY, "bigqueryAccessKey");
    PropertyUtil.getProperties().setProperty(S3_BQ_SECRET_KEY, "bigquerySecretKey");
    PropertyUtil.getProperties().setProperty(S3_BQ_LD_TEMP_BUCKET_NAME, "tempBucket");
    PropertyUtil.getProperties().setProperty(S3_BQ_INPUT_FORMAT, "inputFormat");

    s3ToBigQueryTest = new S3ToBigQuery();
    assertDoesNotThrow(s3ToBigQueryTest::runTemplate);
  }

  @ParameterizedTest
  @MethodSource("propertyKeys")
  void runTemplateWithInvalidParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    s3ToBigQueryTest = new S3ToBigQuery();
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> s3ToBigQueryTest.runTemplate());
    assertEquals(
        "Required parameters for S3toBQ not passed. "
            + "Set mandatory parameter for S3toBQ template"
            + " in resources/conf/template.properties file.",
        exception.getMessage());
  }

  static Stream<String> propertyKeys() {
    return Stream.of(
        PROJECT_ID_PROP,
        S3_BQ_INPUT_LOCATION,
        S3_BQ_OUTPUT_DATASET_NAME,
        S3_BQ_OUTPUT_TABLE_NAME,
        S3_BQ_ACCESS_KEY,
        S3_BQ_SECRET_KEY,
        S3_BQ_LD_TEMP_BUCKET_NAME,
        S3_BQ_INPUT_FORMAT);
  }
}
