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
package com.google.cloud.dataproc.templates.gcs;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class GCStoBigTableConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = PROJECT_ID_PROP)
  @NotEmpty
  private String projectId;

  @JsonProperty(value = GCS_BT_INPUT_LOCATION)
  @NotEmpty
  private String inputFileLocation;

  @JsonProperty(value = GCS_BT_OUTPUT_INSTANCE_ID)
  @NotEmpty
  private String bigTableInstanceId;

  @JsonProperty(value = GCS_BT_OUTPUT_PROJECT_ID)
  @NotEmpty
  private String bigTableProjectId;

  @JsonProperty(value = GCS_BT_INPUT_FORMAT)
  @NotEmpty
  private String inputFileFormat;

  @JsonProperty(value = GCS_BT_CATALOG_LOCATION)
  @NotEmpty
  private String bigTableCatalogLocation;

  @JsonProperty(value = SPARK_BIGTABLE_CREATE_NEW_TABLE)
  private String isCreateBigTable;

  @JsonProperty(value = SPARK_BIGTABLE_BATCH_MUTATE_SIZE)
  private int bigTableBatchMutateSize;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  private String sparkLogLevel;

  public @NotEmpty String getProjectId() {
    return projectId;
  }

  public @NotEmpty String getInputFileLocation() {
    return inputFileLocation;
  }

  public @NotEmpty String getBigTableInstanceId() {
    return bigTableInstanceId;
  }

  public @NotEmpty String getBigTableProjectId() {
    return bigTableProjectId;
  }

  public @NotEmpty String getInputFileFormat() {
    return inputFileFormat;
  }

  public @NotEmpty String getBigTableCatalogLocation() {
    return bigTableCatalogLocation;
  }

  public String getIsCreateBigTable() {
    return isCreateBigTable;
  }

  public int getBigTableBatchMutateSize() {
    return bigTableBatchMutateSize;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  @Override
  public String toString() {
    return "GCStoBigTableConfig{"
        + "projectId='"
        + projectId
        + '\''
        + ", inputFileLocation='"
        + inputFileLocation
        + '\''
        + ", bigTableInstanceId='"
        + bigTableInstanceId
        + '\''
        + ", bigTableProjectId='"
        + bigTableProjectId
        + '\''
        + ", inputFileFormat='"
        + inputFileFormat
        + '\''
        + ", bigTableCatalogLocation='"
        + bigTableCatalogLocation
        + '\''
        + ", isCreateBigTable='"
        + isCreateBigTable
        + '\''
        + ", bigTableBatchMutateSize='"
        + bigTableBatchMutateSize
        + '\''
        + '}';
  }

  public static GCStoBigTableConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, GCStoBigTableConfig.class);
  }

  @AssertTrue(
      message =
          "Required parameters for GCStoBigTable not passed. Refer to databases/README.md for more instructions.")
  private boolean isInputValid() {
    return StringUtils.isNotBlank(bigTableCatalogLocation)
        && StringUtils.isNotBlank(inputFileFormat)
        && StringUtils.isNotBlank(bigTableProjectId)
        && StringUtils.isNotBlank(bigTableInstanceId)
        && StringUtils.isNotBlank(inputFileLocation)
        && StringUtils.isNotBlank(projectId);
  }
}
