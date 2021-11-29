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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import jakarta.validation.ConstraintViolation;
import java.io.IOException;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GeneralTemplateConfigTest {

  public static GeneralTemplateConfig exampleConfig() throws IOException {
    InputConfig inputConfig = new InputConfig();
    inputConfig.setFormat("bigquery");
    inputConfig.setOptions(ImmutableMap.of("table", "project:dataset.table"));

    QueryConfig queryConfig = new QueryConfig();
    queryConfig.setSql("SELECT * FROM logs");

    OutputConfig outputConfig = new OutputConfig();
    outputConfig.setFormat("avro");
    outputConfig.setPath("gs://bucket/output/");

    GeneralTemplateConfig config = new GeneralTemplateConfig();
    config.setInput(ImmutableMap.of("logs", inputConfig));
    config.setQuery(ImmutableMap.of("logs", queryConfig));
    config.setOutput(ImmutableMap.of("logs", outputConfig));
    return config;
  }

  @Test
  void testValidateConfig() throws IOException {
    GeneralTemplateConfig config = exampleConfig();
    assertTrue(config.validate().isEmpty());
  }

  @Test
  void testValidateConfigMissingInput() throws IOException {
    GeneralTemplateConfig config = exampleConfig();
    config.setInput(null);
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    assertEquals(1, violations.size());
    Assertions.assertEquals("must not be empty", violations.iterator().next().getMessage());
    Assertions.assertEquals("input", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void testValidateConfigMissingOutput() throws IOException {
    GeneralTemplateConfig config = exampleConfig();
    config.setOutput(null);
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    assertEquals(1, violations.size());
    Assertions.assertEquals("must not be empty", violations.iterator().next().getMessage());
    Assertions.assertEquals("output", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void testValidateConfigMissingQuery() throws IOException {
    GeneralTemplateConfig config = exampleConfig();
    config.setQuery(null);
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    assertTrue(violations.isEmpty());
  }

}