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

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/** PubSub Partition Reader using PubSub java client library */
public class PubSubPartitionReader implements PartitionReader<InternalRow> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubPartitionReader.class);

  private final Map<String, String> options;
  private SubscriberStub subscriber;
  private Iterator<ReceivedMessage> messageIterator;
  private ReceivedMessage currentMessage;
  private final String projectId;
  private final String subscriptionId;
  private final int maxMessagesPerPull;

  public PubSubPartitionReader(InputPartition partition) {
    this.options = ((PubSubInputPartition) partition).getOptions();
    this.projectId = options.get("projectId");
    this.subscriptionId = options.get("subscriptionId");
    // Define a reasonable max messages per pull for a single task
    this.maxMessagesPerPull = Integer.parseInt(options.getOrDefault("maxMessagesPerPull", "1000"));
  }

  private void pullMessages() throws IOException {
    SubscriberStubSettings.Builder settingsBuilder =
        SubscriberStubSettings.newBuilder()
            .setTransportChannelProvider(
                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                    .setEndpoint(SubscriberStubSettings.getDefaultEndpoint())
                    .setMaxInboundMessageSize(20 * 1024 * 1024) // 20 MB
                    .build());

    this.subscriber = GrpcSubscriberStub.create(settingsBuilder.build());

    String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
    PullRequest pullRequest =
        PullRequest.newBuilder()
            .setMaxMessages(maxMessagesPerPull)
            .setSubscription(subscriptionName)
            .build();

    LOGGER.info(
        "Executor pulling {} messages from {} {}", maxMessagesPerPull, projectId, subscriptionId);
    PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
    this.messageIterator = pullResponse.getReceivedMessagesList().iterator();
  }

  @Override
  public boolean next() throws IOException {
    if (messageIterator == null) {
      LOGGER.debug("First call to next() for this partition: initialize client and pull messages");
      pullMessages();
    }

    if (messageIterator.hasNext()) {
      currentMessage = messageIterator.next();
      return true;
    }
    return false;
  }

  @Override
  public InternalRow get() {
    String ackId = currentMessage.getAckId();
    byte[] data = currentMessage.getMessage().getData().toByteArray();
    String messageId = currentMessage.getMessage().getMessageId();

    // Convert publish time to microseconds since epoch for Spark TimestampType
    long publishTimeMicros =
        TimeUnit.SECONDS.toMicros(currentMessage.getMessage().getPublishTime().getSeconds())
            + TimeUnit.NANOSECONDS.toMicros(
                currentMessage.getMessage().getPublishTime().getNanos());

    LOGGER.debug("Preparing Data List");
    List<Object> list =
        Arrays.asList(
            UTF8String.fromString(ackId),
            UTF8String.fromBytes(data),
            UTF8String.fromString(messageId),
            publishTimeMicros);

    LOGGER.debug("Returning InternalRow");
    return InternalRow.apply(JavaConverters.asScalaBuffer(list).toSeq());
  }

  @Override
  public void close() {
    if (subscriber != null) {
      subscriber.close();
      LOGGER.info("PubSubPartitionReader closed subscriber.");
    }
  }
}
