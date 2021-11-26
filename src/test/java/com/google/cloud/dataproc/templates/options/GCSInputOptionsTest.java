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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.dataproc.templates.options.GCSInputOptions.Format;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSInputOptionsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSInputOptionsTest.class);
  private Validator validator;

  @BeforeEach
  void setup() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();
  }

  @Test
  void testFoobar() {
    Properties props = new Properties();
    props.put("gcs.format", "AVRO");
    props.put("gcs.path", "gs://path...");

    GCSInputOptions options = TemplateOptionsFactory.fromProps(props)
        .as(GCSInputOptions.class).create();
    assertEquals("gcs.", options.getOptionsPrefix());
    assertEquals("gs://path...", options.getPath());
    assertEquals("AVRO", options.getFormat());
    assertEquals(Format.AVRO, options.getFormatEnum());
  }

}
