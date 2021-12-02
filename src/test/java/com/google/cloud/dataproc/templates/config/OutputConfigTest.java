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
package com.google.cloud.dataproc.templates.config;

import static org.junit.jupiter.api.Assertions.*;

import jakarta.validation.ConstraintViolation;
import java.util.Set;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OutputConfigTest {

  @Test
  void testValidate() {
    OutputConfig outputConfig = new OutputConfig();
    outputConfig.setFormat("someformat");
    assertTrue(GeneralTemplateConfig.validate(outputConfig).isEmpty());
  }

  @Test
  void testValidateWithoutFormat() {
    OutputConfig outputConfig = new OutputConfig();
    assertEquals(1, GeneralTemplateConfig.validate(outputConfig).size());
  }

  @ParameterizedTest()
  @ValueSource(strings = {"Append", "ErrorIfExists", "Ignore", "Overwrite"})
  void testValidateWithSaveMode(SaveMode saveMode) {
    OutputConfig outputConfig = new OutputConfig();
    outputConfig.setFormat("someformat");
    outputConfig.setMode(saveMode.name());
    assertTrue(GeneralTemplateConfig.validate(outputConfig).isEmpty());
  }

  @Test
  void testValidateWithInvalidSaveMode() {
    OutputConfig outputConfig = new OutputConfig();
    outputConfig.setFormat("someformat");
    outputConfig.setMode("not a valid save mode");
    Set<ConstraintViolation<OutputConfig>> violations =
        GeneralTemplateConfig.validate(outputConfig);
    assertEquals(1, violations.size());
    assertEquals("mode", violations.iterator().next().getPropertyPath().toString());
    assertEquals(
        "must match \"Overwrite|ErrorIfExists|Append|Ignore\"",
        violations.iterator().next().getMessage());
  }
}
