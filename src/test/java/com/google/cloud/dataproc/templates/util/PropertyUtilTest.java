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
package com.google.cloud.dataproc.templates.util;

import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PropertyUtilTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyUtilTest.class);
  private Properties properties;

  @BeforeEach
  void setUp() {
    LOGGER.info("Setting up PropertyUtilTest. Fetching properties.");
    properties = PropertyUtil.getProperties();
  }

  @AfterEach
  void tearDown() {}

  @Test
  void getProperties() {
    LOGGER.info("Test:getProperties() ");
    PropertyUtil.printAllProperties();
    Assertions.assertNotNull(properties.getProperty(TemplateConstants.PROJECT_ID_PROP));
    Assertions.assertNotNull(properties.getProperty(TemplateConstants.BIGTABLE_KEY_COL_PROP));
    Assertions.assertNotNull(
        properties.getProperty(TemplateConstants.BIGTABLE_COL_FAMILY_NAME_PROP));
    Assertions.assertNotNull(
        properties.getProperty(TemplateConstants.BIGTABLE_OUTPUT_TABLE_NAME_PROP));
    Assertions.assertNotNull(properties.getProperty(TemplateConstants.GCS_STAGING_BUCKET_PATH));
    Assertions.assertNotNull(properties.getProperty(TemplateConstants.BIGTABLE_INSTANCE_ID_PROP));

    Assertions.assertNull(properties.getProperty("non.existent.prop"));
  }

  @Test
  void testRegisterProperties() {
    Properties extraProperties = new Properties();
    extraProperties.put("addingAnExtraProperty", "Added!");
    Assertions.assertNull(PropertyUtil.getProperties().get("addingAnExtraProperty"));

    PropertyUtil.registerProperties(extraProperties);
    Assertions.assertEquals("Added!", PropertyUtil.getProperties().get("addingAnExtraProperty"));
  }
}
