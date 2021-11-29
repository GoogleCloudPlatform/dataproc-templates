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
package com.google.cloud.dataproc.templates.main;

import com.google.cloud.dataproc.templates.BaseTemplate.TemplateName;
import com.google.cloud.dataproc.templates.GenericTemplate;
import com.google.cloud.dataproc.templates.databases.SpannerToGCS;
import com.google.cloud.dataproc.templates.gcs.GCStoBigquery;
import com.google.cloud.dataproc.templates.hive.HiveToBigQuery;
import com.google.cloud.dataproc.templates.hive.HiveToGCS;
import com.google.cloud.dataproc.templates.pubsub.PubSubToBQ;
import com.google.cloud.dataproc.templates.s3.S3ToBigQuery;
import com.google.cloud.dataproc.templates.word.WordCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataProcTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataProcTemplate.class);

  public static void main(String[] args) {
    if (args.length < 1) {
      throw new IllegalArgumentException(
          "Minimum 1 arguments is required: " + "<template-name> Ex wordcount");
    }
    TemplateName template;
    String templateName = args[0].trim().toUpperCase();
    try {
      template = TemplateName.valueOf(templateName);
    } catch (IllegalArgumentException ex) {
      LOGGER.error("Unexpected template name :{}", templateName, ex);
      throw ex;
    }
    LOGGER.info("Running job for {}", template);
    runSparkJob(template);
  }

  /**
   * Run spark job for template.
   *
   * @param template name of the template.
   */
  static void runSparkJob(TemplateName template) {
    LOGGER.debug("Start API runSparkJob");

    switch (template) {
      case WORDCOUNT:
        {
          new WordCount().runTemplate();
        }
        break;
      case GENERIC:
        {
          new GenericTemplate().runTemplate();
        }
        break;
      case HIVETOGCS:
        {
          new HiveToGCS().runTemplate();
        }
        break;
      case PUBSUBTOBQ:
        {
          new PubSubToBQ().runTemplate();
        }
        break;
      case HIVETOBIGQUERY:
        {
          new HiveToBigQuery().runTemplate();
        }
        break;
      case GCSTOBIGQUERY:
        {
          new GCStoBigquery().runTemplate();
        }
        break;
      case SPANNERTOGCS:
        {
          new SpannerToGCS().runTemplate();
        }
        break;
      case S3TOBIGQUERY:
        {
          new S3ToBigQuery().runTemplate();
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected template name :" + template);
    }

    LOGGER.debug("End API runSparkJob");
  }
}
