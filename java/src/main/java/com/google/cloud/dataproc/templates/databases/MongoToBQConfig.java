/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.dataproc.templates.databases;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class MongoToBQConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = MONGO_BQ_INPUT_URI)
  @NotEmpty
  private String inputURI;

  @JsonProperty(value = MONGO_BQ_INPUT_DATABASE)
  @NotEmpty
  private String inputDatabase;

  @JsonProperty(value = MONGO_BQ_INPUT_COLLECTION)
  @NotEmpty
  private String inputCollection;

  @JsonProperty(value = MONGO_BQ_OUTPUT_MODE)
  @NotEmpty
  private String outputMode;

  @JsonProperty(value = MONGO_BQ_OUTPUT_DATASET)
  @NotEmpty
  private String bqOutputDataset;

  @JsonProperty(value = MONGO_BQ_OUTPUT_TABLE)
  @NotEmpty
  private String bqOutputTable;

  @JsonProperty(value = MONGO_BQ_TEMP_BUCKET_NAME)
  @NotEmpty
  private String bqTempBucket;

  public @NotEmpty String getProjectId() {
    return projectId;
  }

  public @NotEmpty String getInputURI() {
    return inputURI;
  }

  public @NotEmpty String getInputDatabase() {
    return inputDatabase;
  }

  public @NotEmpty String getInputCollection() {
    return inputCollection;
  }

  public @NotEmpty String getOutputMode() {
    return outputMode;
  }

  public @NotEmpty String getBqOutputDataset() {
    return bqOutputDataset;
  }

  public @NotEmpty String getBqOutputTable() {
    return bqOutputTable;
  }

  public @NotEmpty String getBqTempBucket() {
    return bqTempBucket;
  }

  @Override
  public String toString() {
    return "MongoToBQConfig{"
        + "projectId='"
        + projectId
        + '\''
        + ", inputURI='"
        + inputURI
        + '\''
        + ", inputDatabase='"
        + inputDatabase
        + '\''
        + ", inputCollection='"
        + inputCollection
        + '\''
        + ", outputMode='"
        + outputMode
        + '\''
        + ", bqOutputDataset='"
        + bqOutputDataset
        + '\''
        + ", bqOutputTable='"
        + bqOutputTable
        + '\''
        + ", bqTempBucket='"
        + bqTempBucket
        + '\''
        + '}';
  }

  public static MongoToBQConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, MongoToBQConfig.class);
  }

  @AssertTrue(
      message =
          "Required parameters for MongoToBQ not passed. Refer to databases/README.md for more instructions.")
  private boolean isInputValid() {
    return StringUtils.isNotBlank(inputURI)
        && StringUtils.isNotBlank(inputDatabase)
        && StringUtils.isNotBlank(inputCollection)
        && StringUtils.isNotBlank(outputMode)
        && StringUtils.isNotBlank(bqOutputDataset)
        && StringUtils.isNotBlank(bqOutputTable)
        && StringUtils.isNotBlank(bqTempBucket);
  }
}
