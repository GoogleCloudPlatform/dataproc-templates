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
package com.google.cloud.dataproc.templates.main;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DataProcTemplateTest {

  @ParameterizedTest
  @MethodSource("stringValidInputArgs")
  void testRunSparkJobWithValidInputArgs(List<String> args) {
    DataProcTemplate.main(args.toArray(new String[0]));
  }

  @ParameterizedTest
  @MethodSource("stringInValidOutputArgs")
  void testRunSparkJobWithInValidInputArgs(List<String> args) {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DataProcTemplate.main(args.toArray(new String[0])));
    assertTrue(exception.getMessage().contains("Unexpected template name: "));
  }


  @Test
  void testRunSparkJobWithoutTemplateOption() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> DataProcTemplate.main(new String[0]));
    assertTrue(exception.getMessage().contains("Missing required option: template"));
  }


  @Test
  public void testTemplateArg() {
    CommandLine cmd = DataProcTemplate.parseArguments("--template", "FOO");
    String template = cmd.getOptionValue("template");
    Assertions.assertEquals("FOO", template);
  }

  @Test
  public void testPropertiesArg() {
    CommandLine cmd =
        DataProcTemplate.parseArguments(
            "--template", "FOO",
            "--prop", "key1=value1",
            "--prop", "key2=value2");
    String template = cmd.getOptionValue("template");
    Properties properties = cmd.getOptionProperties("prop");
    Assertions.assertEquals("FOO", template);
    Assertions.assertFalse(properties.isEmpty());
    Assertions.assertEquals("value1", properties.get("key1"));
    Assertions.assertEquals("value2", properties.get("key2"));
  }

  static Stream<Arguments> stringValidInputArgs() {
    return Stream.of(
        arguments(asList("--template", "HiveToGcs", "b")),
        arguments(asList("--template", "Hivetogcs")),
        arguments(asList("--template", "hivetogcs", " ", "something_else")),
        arguments(asList("--template", "HIVETOGCS")),
        arguments(asList("--template", " hivetogcs")));
  }

  static Stream<Arguments> stringInValidOutputArgs() {
    return Stream.of(
        arguments(asList("--template", " ")),
        arguments(asList("--template", " hive2gcs")),
        arguments(asList("--template", "hive")),
        arguments(asList("--template", "")));
  }

}
