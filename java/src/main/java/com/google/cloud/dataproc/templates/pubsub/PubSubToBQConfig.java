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

public class PubSubToBQConfig {

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

  @JsonProperty(value = PUBSUB_BQ_OUTPUT_PROJECT_ID_PROP)
  @NotEmpty
  private String outputProjectID;

  @JsonProperty(value = PUBSUB_BQ_OUTPUT_DATASET_PROP)
  @NotEmpty
  private String pubSubBQOutputDataset;

  @JsonProperty(value = PUBSUB_BQ_OUTPUT_TABLE_PROP)
  @NotEmpty
  private String pubSubBQOutputTable;

  @JsonProperty(value = PUBSUB_BQ_OUTPUT_GCS_CHECKPOINT_PROP)
  @NotEmpty
  private String pubSubBQOutputGCSCheckpoint;

  @JsonProperty(value = PUBSUB_BQ_BATCH_SIZE_PROP)
  private int batchSize;

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

  public @NotEmpty String getOutputProjectID() {
    return outputProjectID;
  }

  public @NotEmpty String getPubSubBQOutputDataset() {
    return pubSubBQOutputDataset;
  }

  public @NotEmpty String getPubSubBQOutputTable() {
    return pubSubBQOutputTable;
  }

  public @NotEmpty String getPubSubBQOutputGCSCheckpoint() {
    return pubSubBQOutputGCSCheckpoint;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public String getSparkLogLevel() {
    return sparkLogLevel;
  }

  @Override
  public String toString() {
    return "PubSubToBQConfig{"
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
        + ", outputProjectID='"
        + outputProjectID
        + '\''
        + ", pubSubBQOutputDataset='"
        + pubSubBQOutputDataset
        + '\''
        + ", pubSubBQOutputTable='"
        + pubSubBQOutputTable
        + '\''
        + ", pubSubBQOutputGCSCheckpoint='"
        + pubSubBQOutputGCSCheckpoint
        + '\''
        + ", batchSize="
        + batchSize
        + ", sparkLogLevel='"
        + sparkLogLevel
        + '\''
        + '}';
  }

  public static PubSubToBQConfig fromProperties(Properties properties) {
    return mapper.convertValue(properties, PubSubToBQConfig.class);
  }

  @AssertTrue(
      message =
          "Required parameters for PubSubToBQ not passed. Refer to pubsub/README.md for more instructions.")
  private boolean isInputValid() {
    return StringUtils.isNotBlank(inputProjectID)
        && StringUtils.isNotBlank(pubsubInputSubscription)
        && StringUtils.isNotBlank(outputProjectID)
        && StringUtils.isNotBlank(pubSubBQOutputDataset)
        && StringUtils.isNotBlank(pubSubBQOutputTable)
        && StringUtils.isNotBlank(pubSubBQOutputGCSCheckpoint);
  }
}
