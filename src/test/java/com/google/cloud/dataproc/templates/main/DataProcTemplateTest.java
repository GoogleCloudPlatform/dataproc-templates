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
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DataProcTemplateTest {

  @ParameterizedTest
  @MethodSource("stringValidInputArgs")
  void testRunSparkJobWithValidInputArgs(List<String> args) {
    DataProcTemplate.main((String[]) args.toArray());
  }

  @ParameterizedTest
  @MethodSource("stringInValidOutputArgs")
  void testRunSparkJobWithInValidInputArgs(List<String> args) {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> DataProcTemplate.main((String[]) args.toArray()));
    assertTrue(exception.getMessage().contains("No enum constant"));
  }

  static Stream<Arguments> stringValidInputArgs() {
    return Stream.of(
        arguments(asList("HiveToGcs", "b")),
        arguments(asList("Hivetogcs")),
        arguments(asList("hivetogcs", " ", "something_else")),
        arguments(asList("HIVETOGCS")),
        arguments(asList(" hivetogcs")));
  }

  static Stream<Arguments> stringInValidOutputArgs() {
    return Stream.of(
        arguments(asList(" ")),
        arguments(asList(" hive2gcs")),
        arguments(asList("hive")),
        arguments(asList("")));
  }
}
