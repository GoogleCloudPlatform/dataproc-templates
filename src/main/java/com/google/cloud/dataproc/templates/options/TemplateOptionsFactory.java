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

import com.google.common.base.Strings;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemplateOptionsFactory<T extends BaseOptions> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TemplateOptionsFactory.class);

  private final Properties properties;
  private final Class<T> optionsClazz;

  public TemplateOptionsFactory(Properties properties, Class<T> optionsClazz) {
    this.properties = properties;
    this.optionsClazz = optionsClazz;
  }

  public static TemplateOptionsFactory<TemplateOptions> fromProps(Properties properties) {
    return new TemplateOptionsFactory<>(properties, TemplateOptions.class);
  }

  public <Opts extends BaseOptions> TemplateOptionsFactory<Opts> as(Class<Opts> optionsClazz) {
    return new TemplateOptionsFactory<>(this.properties, optionsClazz);
  }

  public static Properties unscopeProperties(Properties properties, String prefix) {
    if (Strings.isNullOrEmpty(prefix)) {
      return properties;
    }
    // remove the qualifier prefix
    Properties props = new Properties();
    for (Entry<?, ?> entry : properties.entrySet()) {
      String key = entry.getKey().toString();
      if (key.startsWith(prefix)) {
        props.put(key.substring(prefix.length()), entry.getValue());
      }
    }
    if (props.isEmpty()) {
      LOGGER.warn("No matching properties remaining after stripping options prefix \"{}\"", prefix);
    }
    return props;
  }

  public T create() {
    T optionsBean;
    try {
      optionsBean = optionsClazz.getConstructor().newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Could not construct options bean %s", optionsClazz), e);
    }
    LOGGER.info(
        "Creating options bean {} from properties {} with scope {}",
        optionsClazz,
        properties,
        optionsBean);
    Map<?, ?> props = unscopeProperties(properties, optionsBean.getOptionsPrefix());
    try {
      BeanUtils.populate(optionsBean, props);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException(
          String.format("Could not populate properties to options bean %s", optionsClazz), e);
    }
    return validate(optionsBean);
  }

  public T validate(T optionsBean) {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<T>> violations = validator.validate(optionsBean);
    for (ConstraintViolation<T> violation : violations) {
      LOGGER.error(
          "Option violation for: {}; {}; in {}",
          violation.getPropertyPath(),
          violation.getMessage(),
          violation.getRootBeanClass());
    }
    if (!violations.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Options could not be validated, %s", violations));
    }
    return optionsBean;
  }
}
