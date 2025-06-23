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
package com.google.cloud.dataproc.templates.pubsub.internal;

import com.google.gson.Gson;
import org.apache.spark.sql.connector.read.streaming.Offset;

/** We will maintain basic offset management. */
public class PubSubOffset extends Offset {

  private final long batchId;
  private static final Gson GSON = new Gson();

  public PubSubOffset(long batchId) {
    this.batchId = batchId;
  }

  public long getBatchId() {
    return batchId;
  }

  @Override
  public String json() {
    return GSON.toJson(this);
  }

  @Override
  public String toString() {
    return "PubSubOffset{" + "batchId=" + batchId + '}';
  }
}
