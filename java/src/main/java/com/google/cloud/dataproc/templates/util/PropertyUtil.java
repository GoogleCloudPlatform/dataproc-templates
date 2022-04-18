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

import static com.google.cloud.dataproc.templates.util.TemplateConstants.DEFAULT_PROPERTY_FILE;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util class to read and maintain all properties. */
public class PropertyUtil {

  private static Properties properties;
  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyUtil.class);

  /** API to return property file. */
  public static Properties getProperties() {
    if (Objects.isNull(properties)) {
      synchronized (PropertyUtil.class) {
        properties = loadProperties();
      }
    }
    return properties;
  }

  /** Helper function to load default property file */
  private static Properties loadProperties() {
    if (Objects.isNull(properties)) {
      // for static access, uses the class name directly
      InputStream is =
          PropertyUtil.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTY_FILE);
      properties = new Properties();
      try {
        properties.load(is);
      } catch (IOException e) {
        LOGGER.error("Error loading property file: {}", LOGGER);
      }
    }
    return properties;
  }

  /** Helper function to override default properties */
  public static void registerProperties(Properties properties) {
    Properties defaults = getProperties();
    defaults.putAll(properties);
  }

  /** Print all property key, value pairs to logger at info level. */
  public static void printAllProperties() {
    if (Objects.isNull(properties)) {
      getProperties();
    }
    LOGGER.info("Logging all properties");
    properties.forEach(
        (k, v) -> {
          LOGGER.info("{}:{}", k, v);
        });
  }
}
