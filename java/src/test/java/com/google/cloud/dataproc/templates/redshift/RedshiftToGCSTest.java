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
package com.google.cloud.dataproc.templates.redshift;

import static com.google.cloud.dataproc.templates.databases.RedshiftToGCSConfig.*;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.dataproc.templates.databases.RedshiftToGCS;
import com.google.cloud.dataproc.templates.databases.RedshiftToGCSConfig;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil.ValidationException;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RedshiftToGCSTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(com.google.cloud.dataproc.templates.redshift.RedshiftToGCSTest.class);

  @BeforeEach
  void setUp() {
    PropertyUtil.getProperties().setProperty(REDSHIFT_AWS_INPUT_URL, "someValue");
    PropertyUtil.getProperties().setProperty(REDSHIFT_AWS_INPUT_TABLE, "someValue");
    PropertyUtil.getProperties().setProperty(REDSHIFT_AWS_TEMP_DIR, "someValue");
    PropertyUtil.getProperties().setProperty(REDSHIFT_AWS_INPUT_IAM_ROLE, "someValue");
    PropertyUtil.getProperties().setProperty(REDSHIFT_AWS_INPUT_ACCESS_KEY, "someValue");
    PropertyUtil.getProperties().setProperty(REDSHIFT_AWS_INPUT_SECRET_KEY, "someValue");
    PropertyUtil.getProperties().setProperty(REDSHIFT_GCS_OUTPUT_FILE_FORMAT, "csv");
    PropertyUtil.getProperties()
        .setProperty(REDSHIFT_GCS_OUTPUT_FILE_LOCATION, "gs://bucket_name/folder_name");
    PropertyUtil.getProperties().setProperty(REDSHIFT_GCS_OUTPUT_MODE, "overwrite");
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    RedshiftToGCSConfig config = RedshiftToGCSConfig.fromProperties(PropertyUtil.getProperties());
    RedshiftToGCS template = new RedshiftToGCS(config);
    assertDoesNotThrow(template::validateInput);
  }

  @ParameterizedTest
  @MethodSource("requiredPropertyKeys")
  void runTemplateWithMissingRequiredParameters(String propKey) {
    LOGGER.info("Running test: runTemplateWithInvalidParameters");
    PropertyUtil.getProperties().setProperty(propKey, "");
    RedshiftToGCSConfig config = RedshiftToGCSConfig.fromProperties(PropertyUtil.getProperties());
    RedshiftToGCS template = new RedshiftToGCS(config);
    ValidationException exception =
        assertThrows(ValidationException.class, template::validateInput);
  }

  static Stream<String> requiredPropertyKeys() {
    return Stream.of(
        REDSHIFT_AWS_INPUT_URL,
        REDSHIFT_AWS_INPUT_TABLE,
        REDSHIFT_AWS_TEMP_DIR,
        REDSHIFT_AWS_INPUT_IAM_ROLE,
        REDSHIFT_AWS_INPUT_ACCESS_KEY,
        REDSHIFT_AWS_INPUT_SECRET_KEY,
        REDSHIFT_GCS_OUTPUT_FILE_FORMAT,
        REDSHIFT_GCS_OUTPUT_FILE_LOCATION,
        REDSHIFT_GCS_OUTPUT_MODE);
  }
}
