/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.dataproc.templates.pubsub;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class PubSubToBigTableConfig {

  static final ObjectMapper mapper =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = PUBSUB_INPUT_PROJECT_ID_PROP)
  @NotEmpty
  private String inputProjectID;

  @JsonProperty(value = PUBSUB_INPUT_SUBSCRIPTION_PROP)
  @NotEmpty
  private String pubsubInputSubscription;

  @JsonProperty(value = PUBSUB_TIMEOUT_MS_PROP)
  @Min(value = 2000)
  private long timeoutMs;

  @JsonProperty(value = PUBSUB_STREAMING_DURATION_SECONDS_PROP)
  private int streamingDuration;

  @JsonProperty(value = PUBSUB_TOTAL_RECEIVERS_PROP)
  @Min(value = 1)
  private int totalReceivers;

  @JsonProperty(value = PUBSUB_BIGTABLE_OUTPUT_PROJECT_ID_PROP)
  @NotEmpty
  private String pubSubBigTableOutputProjectId;

  @JsonProperty(value = PUBSUB_BIGTABLE_OUTPUT_INSTANCE_ID_PROP)
  @NotEmpty
  private String pubSubBigTableOutputInstanceId;

  @JsonProperty(value = PUBSUB_BIGTABLE_OUTPUT_TABLE_PROP)
  @NotEmpty
  private String pubSubBigTableOutputTable;

  @JsonProperty(value = PUBSUB_BIGTABLE_BATCH_SIZE_PROP)
  private int batchSize;

  @JsonProperty(value = PUBSUB_BIGTABLE_CATALOG_LOCATION_PROP)
  @NotEmpty
  private String pubSubBigTableCatalogLocation;

  @JsonProperty(value = SPARK_LOG_LEVEL)
  @Pattern(regexp = "ALL|DEBUG|ERROR|FATAL|INFO|OFF|TRACE|WARN")
  private String sparkLogLevel;

  public @NotEmpty String getInputProjectID() {
    return inputProjectID;
  }

  public @NotEmpty String getPubsubInputSubscription() {
    return pubsubInputSubscription;
  }

  public long getTimeoutMs() {
    return timeoutMs;
  }

  public int getStreamingDuration() {
    return streamingDuration;
  }

  public int getTotalReceivers() {
    return totalReceivers;
  }

  public @NotEmpty String getPubSubBigTableOutputProjectId() {
    return pubSubBigTableOutputProjectId;
  }

  public @NotEmpty String getPubSubBigTableOutputInstanceId() {
    return pubSubBigTableOutputInstanceId;
  }

  public @NotEmpty String getPubSubBigTableOutputTable() {
    return pubSubBigTableOutputTable;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public @NotEmpty String getPubSubBigTableCatalogLocation() {
    return pubSubBigTableCatalogLocation;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  @Override
  public String toString() {
    return "PubSubToBigTableConfig{"
        + "inputProjectID='"
        + inputProjectID
        + '\''
        + ", pubsubInputSubscription='"
        + pubsubInputSubscription
        + '\''
        + ", timeoutMs="
        + timeoutMs
        + ", streamingDuration="
        + streamingDuration
        + ", totalReceivers="
        + totalReceivers
        + ", pubSubBigTableOutputProjectId='"
        + pubSubBigTableOutputProjectId
        + '\''
        + ", pubSubBigTableOutputInstanceId='"
        + pubSubBigTableOutputInstanceId
        + '\''
        + ", pubSubBigTableOutputTable='"
        + pubSubBigTableOutputTable
        + '\''
        + ", batchSize="
        + batchSize
        + ", pubSubBigTableCatalogLocation='"
        + pubSubBigTableCatalogLocation
        + '\''
        + ", sparkLogLevel='"
        + sparkLogLevel
        + '\''
        + '}';
  }

  public static PubSubToBigTableConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, PubSubToBigTableConfig.class);
  }

  @AssertTrue(
      message =
          "Required parameters for PubSubToBQ not passed. Refer to pubsub/README.md for more instructions.")
  private boolean isInputValid() {
    return StringUtils.isNotBlank(inputProjectID)
        && StringUtils.isNotBlank(pubsubInputSubscription)
        && StringUtils.isNotBlank(pubSubBigTableOutputProjectId)
        && StringUtils.isNotBlank(pubSubBigTableOutputInstanceId)
        && StringUtils.isNotBlank(pubSubBigTableOutputTable)
        && StringUtils.isNotBlank(pubSubBigTableCatalogLocation);
  }
}
