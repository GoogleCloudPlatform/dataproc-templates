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
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Acknowledge retrieved messages from PubSub so pubsub don't send processed messages again. */
public class PubSubAcker implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubAcker.class);

  // Acknowledges messages to Pub/Sub based on the 'ackId' column in the DataFrame.
  public static void acknowledge(Dataset<Row> batchDFWithAckIds, Map<String, String> options) {
    String projectId = options.get("projectId");
    String subscriptionId = options.get("subscriptionId");

    batchDFWithAckIds
        .select("ackId")
        .foreachPartition(
            partition -> {
              // Each partition gets its own Pub/Sub client for parallel acknowledgement
              SubscriberStub subscriber = null;
              try {
                SubscriberStubSettings.Builder settingsBuilder =
                    SubscriberStubSettings.newBuilder()
                        .setTransportChannelProvider(
                            SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                .setEndpoint(SubscriberStubSettings.getDefaultEndpoint())
                                .build());

                subscriber = GrpcSubscriberStub.create(settingsBuilder.build());

                String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
                List<String> ackIdsList = new ArrayList<>();
                // Get ackId from first column
                partition.forEachRemaining(row -> ackIdsList.add(row.getString(0)));

                if (!ackIdsList.isEmpty()) {
                  AcknowledgeRequest acknowledgeRequest =
                      AcknowledgeRequest.newBuilder()
                          .setSubscription(subscriptionName)
                          .addAllAckIds(ackIdsList)
                          .build();
                  subscriber.acknowledgeCallable().call(acknowledgeRequest);
                  LOGGER.info(
                      "Acknowledged {} messages for project {} and subscription {}",
                      ackIdsList.size(),
                      projectId,
                      subscriptionId);
                } else {
                  LOGGER.info("No ackIds to acknowledge for this partition.");
                }
              } catch (IOException e) {
                LOGGER.error("Error acknowledging messages: ", e);
                throw new RuntimeException("Failed to acknowledge Pub/Sub messages.", e);
              } finally {
                if (subscriber != null) {
                  subscriber.close();
                }
              }
            });
  }
}
