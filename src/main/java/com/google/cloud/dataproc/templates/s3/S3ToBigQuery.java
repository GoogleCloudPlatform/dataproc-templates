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
package com.google.cloud.dataproc.templates.s3;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ToBigQuery implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(S3ToBigQuery.class);

  private String projectID;
  private String inputFileLocation;
  private String accessKey;
  private String accessSecret;

  public S3ToBigQuery() {

    projectID = getProperties().getProperty(PROJECT_ID_PROP);
    inputFileLocation = getProperties().getProperty(S3_BQ_INPUT_LOCATION);
    accessKey = getProperties().getProperty(S3_BQ_ACCESS_KEY);
    accessSecret = getProperties().getProperty(S3_BQ_SECRET_KEY);
  }

  @Override
  public void runTemplate() {
    if (StringUtils.isAllBlank(projectID)
        || StringUtils.isAllBlank(inputFileLocation)
        || StringUtils.isAllBlank(accessKey)
        || StringUtils.isAllBlank(accessSecret)) {
      LOGGER.error(
          "{},{},{},{} are required parameter. ",
          PROJECT_ID_PROP,
          S3_BQ_INPUT_LOCATION,
          S3_BQ_ACCESS_KEY,
          S3_BQ_SECRET_KEY_CONFIG_NAME);
      throw new IllegalArgumentException(
          "Required parameters for S3toBQ not passed. "
              + "Set mandatory parameter for S3toBQ template "
              + "in resources/conf/template.properties file.");
    }

    SparkSession spark = null;
    LOGGER.info(
        "Starting S3 to Bigquery spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}",
        S3_BQ_INPUT_LOCATION,
        inputFileLocation,
        S3_BQ_ACCESS_KEY,
        accessKey,
        S3_BQ_SECRET_KEY_CONFIG_NAME,
        accessSecret);

    try {
      spark = SparkSession.builder().appName("S3 to Bigquery load").getOrCreate();

      spark.sparkContext().hadoopConfiguration().set(S3_BQ_ACCESS_KEY_CONFIG_NAME, accessKey);
      spark.sparkContext().hadoopConfiguration().set(S3_BQ_SECRET_KEY_CONFIG_NAME, accessSecret);
      spark
          .sparkContext()
          .hadoopConfiguration()
          .set(S3_BQ_ENDPOINT_CONFIG_NAME, S3_BQ_ENDPOINT_CONFIG_VALUE);

      Dataset<Row> inputData = spark.read().text(inputFileLocation);
      inputData.show();

    } catch (Throwable th) {
      LOGGER.error("Exception in S3toBigquery", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
