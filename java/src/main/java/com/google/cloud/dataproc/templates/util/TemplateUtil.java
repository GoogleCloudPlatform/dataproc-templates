/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.dataproc.templates.util;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.spark.bigquery.repackaged.com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.spark.bigquery.repackaged.com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQuery;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.collect.ImmutableMap;

public class TemplateUtil {
  private static final String USER_AGENT_HEADER = "user-agent";
  private static final String USER_AGENT_VALUE = "google-pso-tool/dataproc-templates/0.1.0";

  public static void trackTemplateInvocation(BaseTemplate.TemplateName templateName) {
    try {
      HeaderProvider headerProvider =
          FixedHeaderProvider.create(
              ImmutableMap.of(USER_AGENT_HEADER, USER_AGENT_VALUE + "-" + templateName));

      BigQuery bigquery =
          BigQueryOptions.newBuilder().setHeaderProvider(headerProvider).build().getService();
      bigquery.listDatasets("bigquery-public-data", BigQuery.DatasetListOption.pageSize(1));
    } catch (Throwable e) {
      // do nothing
    }
  }
}
