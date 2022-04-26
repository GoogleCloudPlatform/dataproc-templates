/*
 * Copyright (C) 2022 Google LLC
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

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationUtil.class);

  public static String getJsonPropertyAnnotationValue(Class<?> clazz, String fieldName) {
    try {
      Field field = clazz.getDeclaredField(fieldName);
      if (field.isAnnotationPresent(JsonProperty.class)) {
        return field.getAnnotation(JsonProperty.class).value();
      }
      return null;
    } catch (NoSuchFieldException e) {
      return null;
    }
  }

  /**
   * Validate jakarta.annotations on any pojo
   *
   * @param object Plain old java object to be validated
   * @param <T> type
   * @return Set of violated constraints
   */
  public static <T> Set<ConstraintViolation<T>> validate(T object) {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<T>> violations = validator.validate(object);
    Class<?> clazz = object.getClass();
    for (ConstraintViolation<T> violation : violations) {
      String annotationValue =
          getJsonPropertyAnnotationValue(clazz, violation.getPropertyPath().toString());
      LOGGER.error(
          "Violation: {} {}; {}; in {}",
          violation.getPropertyPath(),
          annotationValue,
          violation.getMessage(),
          violation.getRootBeanClass());
    }
    return violations;
  }

  public static <T> void validateOrThrow(T object) {
    Set<ConstraintViolation<T>> violations = validate(object);
    if (!violations.isEmpty()) {
      throw new ValidationException(object, new ArrayList<>(validate(object)));
    }
  }

  public static class ValidationException extends IllegalArgumentException {

    private final List<ConstraintViolation<?>> violations;

    public ValidationException(Object object, List<ConstraintViolation<?>> violations) {
      super(String.format("Invalid object %s, violations %s", object, violations));
      this.violations = violations;
    }

    public List<ConstraintViolation<?>> getViolations() {
      return violations;
    }
  }
}
