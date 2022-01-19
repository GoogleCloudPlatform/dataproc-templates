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

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationUtil.class);

  /**
   * Validate jakarta.annotations on any pojo
   *
   * @param pojo Plain old java object to be validated
   * @param <T>  type
   * @return Set of violated constraints
   */
  public static <T> Set<ConstraintViolation<T>> validate(T pojo) {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<T>> violations = validator.validate(pojo);
    for (ConstraintViolation<T> violation : violations) {
      LOGGER.error(
          "Violation: {}; {}; in {}",
          violation.getPropertyPath(),
          violation.getMessage(),
          violation.getRootBeanClass());
    }
    return violations;
  }

  public static <T> void validateOrThrow(T pojo) {
    Set<ConstraintViolation<T>> violations = validate(pojo);
    if (!violations.isEmpty()) {
      throw new ValidationException(pojo, new ArrayList<>(validate(pojo)));
    }
  }

  public static class ValidationException extends IllegalArgumentException {

    private final List<ConstraintViolation<?>> violations;

    public ValidationException(Object pojo, List<ConstraintViolation<?>> violations) {
      super(String.format("Invalid object %s, violations %s", pojo, violations));
      this.violations = violations;
    }

    public List<ConstraintViolation<?>> getViolations() {
      return violations;
    }
  }
}
