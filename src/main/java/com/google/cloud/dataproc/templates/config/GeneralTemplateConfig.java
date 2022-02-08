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

import com.google.cloud.dataproc.templates.config.GeneralTemplateConfig.GeneralTemplateConfigAnnotation;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Payload;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@GeneralTemplateConfigAnnotation
public class GeneralTemplateConfig {

  @NotEmpty() private Map<String, @Valid InputConfig> input = ImmutableMap.of();
  @NotNull private Map<String, @Valid QueryConfig> query = ImmutableMap.of();;
  @NotEmpty() private Map<String, @Valid OutputConfig> output = ImmutableMap.of();

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
    return ValidationUtil.validate(pojo);
  }

  @Constraint(validatedBy = GeneralTemplateConfigValidator.class)
  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface GeneralTemplateConfigAnnotation {

    String message() default "inconsistent config";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
  }

  public static class GeneralTemplateConfigValidator
      implements ConstraintValidator<GeneralTemplateConfigAnnotation, GeneralTemplateConfig> {

    @Override
    public void initialize(GeneralTemplateConfigAnnotation constraintAnnotation) {}

    @Override
    public boolean isValid(GeneralTemplateConfig value, ConstraintValidatorContext context) {
      Map<String, InputConfig> input =
          Optional.ofNullable(value.getInput()).orElse(new HashMap<>());
      Map<String, QueryConfig> query =
          Optional.ofNullable(value.getQuery()).orElse(new HashMap<>());
      Map<String, OutputConfig> output =
          Optional.ofNullable(value.getOutput()).orElse(new HashMap<>());
      for (String name : query.keySet()) {
        if (input.containsKey(name)) {
          context
              .buildConstraintViolationWithTemplate("name conflicts with an input")
              .addPropertyNode("query")
              .addBeanNode()
              .inIterable()
              .atKey(name)
              .addConstraintViolation();
          return false;
        }
      }
      for (String name : output.keySet()) {
        if (!input.containsKey(name) && !query.containsKey(name)) {
          context
              .buildConstraintViolationWithTemplate("name not found as input or query")
              .addPropertyNode("output")
              .addBeanNode()
              .inIterable()
              .atKey(name)
              .addConstraintViolation();
          return false;
        }
      }
      return true;
    }
  }
}
