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
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is the class that will pull messages and synchronize with spark streaming threads */
public class PubSubMicroBatchStream implements MicroBatchStream {

  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMicroBatchStream.class);

  private final CaseInsensitiveStringMap options;
  private long currentReadOffset;
  private long lastCommittedBatchId = -1L;
  private static final Gson GSON = new Gson();

  public PubSubMicroBatchStream(CaseInsensitiveStringMap options, String checkpointLocation) {
    this.options = options;
    this.currentReadOffset = 0L;
  }

  @Override
  public Offset latestOffset() {
    currentReadOffset++;
    LOGGER.info("latestOffset: Returning {}", currentReadOffset);
    return new PubSubOffset(currentReadOffset);
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {

    long startBatch = ((PubSubOffset) start).getBatchId();
    long endBatch = ((PubSubOffset) end).getBatchId();

    LOGGER.info("startBatch:{} endBatch:{}", startBatch, endBatch);

    int numPartitions = options.getInt("numPartitions", 4);
    InputPartition[] partitions = new InputPartition[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      partitions[i] = new PubSubInputPartition(options.asCaseSensitiveMap());
    }

    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new PubSubPartitionReaderFactory();
  }

  @Override
  public Offset initialOffset() {
    return new PubSubOffset(-1L);
  }

  @Override
  public Offset deserializeOffset(String s) {

    PubSubOffset deserialized = GSON.fromJson(s, PubSubOffset.class);
    this.currentReadOffset = Math.max(this.currentReadOffset, deserialized.getBatchId() + 1);
    this.lastCommittedBatchId = deserialized.getBatchId();
    LOGGER.info(
        "deserializeOffset: Deserialized {}. currentReadOffset set to {}",
        deserialized.getBatchId(),
        this.currentReadOffset);

    return deserialized;
  }

  @Override
  public void commit(Offset offset) {

    this.lastCommittedBatchId = ((PubSubOffset) offset).getBatchId();
    LOGGER.info("lastCommittedBatchId:{}", lastCommittedBatchId);
  }

  @Override
  public void stop() {
    LOGGER.info("stop current stream");
  }
}
