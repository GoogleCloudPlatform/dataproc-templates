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
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneralTemplateConfigTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeneralTemplateConfigTest.class);

  public static InputConfig exampleInputConfig() {
    InputConfig inputConfig = new InputConfig();
    inputConfig.setFormat("bigquery");
    inputConfig.setOptions(ImmutableMap.of("table", "project:dataset.table"));
    return inputConfig;
  }

  public static QueryConfig exampleQueryConfig() {
    QueryConfig queryConfig = new QueryConfig();
    queryConfig.setSql("SELECT * FROM logs");
    return queryConfig;
  }

  public static OutputConfig exampleOutputConfig() {
    OutputConfig outputConfig = new OutputConfig();
    outputConfig.setFormat("avro");
    outputConfig.setPath("gs://bucket/output/");
    return outputConfig;
  }

  public static GeneralTemplateConfig exampleConfig() {
    GeneralTemplateConfig config = new GeneralTemplateConfig();
    config.setInput(ImmutableMap.of("logs", exampleInputConfig()));
    config.setQuery(ImmutableMap.of("query_output", exampleQueryConfig()));
    config.setOutput(ImmutableMap.of("query_output", exampleOutputConfig()));
    return config;
  }

  @Test
  void testValidateConfig() {
    GeneralTemplateConfig config = exampleConfig();
    assertTrue(config.validate().isEmpty());
  }

  @Test
  void testValidateConfigMissingInput() {
    GeneralTemplateConfig config = new GeneralTemplateConfig();
    config.setQuery(ImmutableMap.of("query_output", exampleQueryConfig()));
    config.setOutput(ImmutableMap.of("query_output", exampleOutputConfig()));
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    assertEquals(1, violations.size());
    Assertions.assertEquals("must not be empty", violations.iterator().next().getMessage());
    Assertions.assertEquals("input", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void testValidateConfigQueryConflictsWithInputInput() {
    GeneralTemplateConfig config = new GeneralTemplateConfig();
    config.setInput(ImmutableMap.of("logs", exampleInputConfig()));
    config.setQuery(ImmutableMap.of("logs", exampleQueryConfig()));
    config.setOutput(ImmutableMap.of("query_output", exampleOutputConfig()));
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    for (ConstraintViolation<GeneralTemplateConfig> violation : violations) {
      LOGGER.info("'{}'", violation.getMessage());
      if (violation.getMessageTemplate().equals("inconsistent config")) {
        continue;
      }
      Assertions.assertEquals("name conflicts with an input", violation.getMessage());
      Assertions.assertEquals("query[logs]", violation.getPropertyPath().toString());
    }
  }

  @Test
  void testValidateConfigOutputNoMatching() {
    GeneralTemplateConfig config = new GeneralTemplateConfig();
    config.setInput(ImmutableMap.of("logs", exampleInputConfig()));
    config.setQuery(ImmutableMap.of("query_output", exampleQueryConfig()));
    config.setOutput(ImmutableMap.of("unknown", exampleOutputConfig()));
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    assertEquals(2, violations.size());
    for (ConstraintViolation<GeneralTemplateConfig> violation : violations) {
      LOGGER.info("'{}'", violation.getMessage());
      if (violation.getMessageTemplate().equals("inconsistent config")) {
        continue;
      }
      Assertions.assertEquals("name not found as input or query", violation.getMessage());
      Assertions.assertEquals("output[unknown]", violation.getPropertyPath().toString());
    }
  }

  @Test
  void testValidateConfigMissingOutput() {
    GeneralTemplateConfig config = new GeneralTemplateConfig();
    config.setInput(ImmutableMap.of("logs", exampleInputConfig()));
    config.setQuery(ImmutableMap.of("query_output", exampleQueryConfig()));

    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    assertEquals(1, violations.size());
    Assertions.assertEquals("must not be empty", violations.iterator().next().getMessage());
    Assertions.assertEquals("output", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void testValidateConfigMissingQuery() {
    GeneralTemplateConfig config = new GeneralTemplateConfig();
    config.setInput(ImmutableMap.of("logs", exampleInputConfig()));
    config.setOutput(ImmutableMap.of("logs", exampleOutputConfig()));
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    assertTrue(violations.isEmpty());
  }
}
