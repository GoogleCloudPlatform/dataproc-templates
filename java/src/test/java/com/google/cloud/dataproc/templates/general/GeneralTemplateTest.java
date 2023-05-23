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
package com.google.cloud.dataproc.templates.general;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.cloud.dataproc.templates.config.GeneralTemplateConfig;
import com.google.cloud.dataproc.templates.config.GeneralTemplateConfigTest;
import jakarta.validation.ConstraintViolation;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneralTemplateTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeneralTemplateTest.class);

  public static void writeConfigYaml(GeneralTemplateConfig config, Path path) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.findAndRegisterModules();
    mapper.writer().writeValue(path.toFile(), config);
  }

  public static GeneralTemplateConfig exampleConfig() throws IOException {
    return GeneralTemplateConfigTest.exampleConfig();
  }

  public void testLoadConfig(@TempDir Path tempDir) throws IOException {
    Path yamlPath = Paths.get(tempDir.toString(), "config.yaml");
    writeConfigYaml(exampleConfig(), yamlPath);
    LOGGER.info("{}", new String(Files.readAllBytes(yamlPath), StandardCharsets.UTF_8));
    GeneralTemplateConfig config = GeneralTemplate.loadConfigYaml(yamlPath);
    LOGGER.info("{}", config);
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    Assertions.assertTrue(violations.isEmpty());
  }

  public void testLoadConfigWithoutQuery(@TempDir Path tempDir) throws IOException {
    Path yamlPath = Paths.get(tempDir.toString(), "config.yaml");
    String yaml =
        ""
            + "input:\n"
            + "  test:\n"
            + "    format: bigquery\n"
            + "    options: {}\n"
            + "output:\n"
            + "  test:\n"
            + "    format: bigquery\n"
            + "    options: {}\n";
    try (BufferedWriter writer = Files.newBufferedWriter(yamlPath, StandardCharsets.UTF_8)) {
      writer.write(yaml);
    }
    LOGGER.info("{}", new String(Files.readAllBytes(yamlPath), StandardCharsets.UTF_8));
    GeneralTemplateConfig config = GeneralTemplate.loadConfigYaml(yamlPath);
    LOGGER.info("{}", config);
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    Assertions.assertTrue(violations.isEmpty());
  }

  @Ignore
  public void testLoadConfigWithoutInput(@TempDir Path tempDir) throws IOException {
    Path yamlPath = Paths.get(tempDir.toString(), "config.yaml");
    String yaml =
        ""
            + "query:\n"
            + "  result:\n"
            + "    sql: select * from test\n"
            + "output:\n"
            + "  result:\n"
            + "    format: bigquery\n"
            + "    options: {}\n";
    try (BufferedWriter writer = Files.newBufferedWriter(yamlPath, StandardCharsets.UTF_8)) {
      writer.write(yaml);
    }
    LOGGER.info("{}", new String(Files.readAllBytes(yamlPath), StandardCharsets.UTF_8));
    GeneralTemplateConfig config = GeneralTemplate.loadConfigYaml(yamlPath);
    LOGGER.info("{}", config);
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    Assertions.assertFalse(violations.isEmpty());
  }

  public void testLoadConfigWithoutOutput(@TempDir Path tempDir) throws IOException {
    Path yamlPath = Paths.get(tempDir.toString(), "config.yaml");
    String yaml =
        ""
            + "input:\n"
            + "  test:\n"
            + "    format: bigquery\n"
            + "    options: {}\n"
            + "query:\n"
            + "  result:\n"
            + "    sql: select * from test\n";
    try (BufferedWriter writer = Files.newBufferedWriter(yamlPath, StandardCharsets.UTF_8)) {
      writer.write(yaml);
    }
    LOGGER.info("{}", new String(Files.readAllBytes(yamlPath), StandardCharsets.UTF_8));
    GeneralTemplateConfig config = GeneralTemplate.loadConfigYaml(yamlPath);
    LOGGER.info("{}", config);
    Set<ConstraintViolation<GeneralTemplateConfig>> violations = config.validate();
    Assertions.assertFalse(violations.isEmpty());
  }

  @Test
  public void testParseArguments() throws IOException {
    CommandLine cmd = GeneralTemplate.parseArguments("--config", "config.yaml", "--dryrun");
    assertTrue(cmd.hasOption("config"));
    assertEquals("config.yaml", cmd.getOptionValue("config"));
    assertTrue(cmd.hasOption("dryrun"));
  }
}
