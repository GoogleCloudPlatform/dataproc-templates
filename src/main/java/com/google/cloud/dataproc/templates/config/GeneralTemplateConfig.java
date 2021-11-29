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
package com.google.cloud.dataproc.templates.config;

import com.google.common.base.MoreObjects;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Valid;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.constraints.NotEmpty;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneralTemplateConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeneralTemplateConfig.class);

  @NotEmpty()
  private Map<String, @Valid InputConfig> input;
  @NotEmpty()
  private Map<String, @Valid OutputConfig> output;
  private Map<String, @Valid QueryConfig> query;

  public Map<String, InputConfig> getInput() {
    return input;
  }

  public void setInput(Map<String, InputConfig> input) {
    this.input = input;
  }

  public Map<String, OutputConfig> getOutput() {
    return output;
  }

  public void setOutput(Map<String, OutputConfig> output) {
    this.output = output;
  }

  public Map<String, QueryConfig> getQuery() {
    return query;
  }

  public void setQuery(Map<String, QueryConfig> query) {
    this.query = query;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("input", input)
        .add("output", output)
        .add("query", query)
        .toString();
  }

  public Set<ConstraintViolation<GeneralTemplateConfig>> validate() {
    return validate(this);
  }

  static <T> Set<ConstraintViolation<T>> validate(T pojo) {
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
}
