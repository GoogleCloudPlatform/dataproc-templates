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
package com.google.cloud.dataproc.templates.options;

import static org.junit.jupiter.api.Assertions.*;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryOutputOptionsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryOutputOptionsTest.class);
  private Validator validator;

  @BeforeEach
  void setup() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();
  }

  @Test
  void testFoobar() throws InvocationTargetException, IllegalAccessException {
    Properties props = new Properties();
    props.put("bigquery.table", "table name");
    props.put("bigquery.temporaryGcsBucket", "bucket");

    BigQueryOutputOptions options = TemplateOptionsFactory.fromProps(props)
        .as(BigQueryOutputOptions.class).create();
    assertEquals("bigquery.", options.getOptionsPrefix());
    assertEquals("bucket", options.getTemporaryGcsBucket());
    assertEquals("table name", options.getTable());

  }

}
