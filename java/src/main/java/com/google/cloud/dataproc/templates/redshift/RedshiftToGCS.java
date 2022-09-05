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
package com.google.cloud.dataproc.templates.redshift;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.templates.BaseTemplate;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedshiftToGCS implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftToGCS.class);

  private final String inputUrl;
  private final String inputTable;
  private final String tempDir;
  private final String iamRole;
  private final String accessKey;
  private final String secretKey;
  private final String fileFormat;
  private final String fileLocation;
  private final String outputMode;

  public RedshiftToGCS() {
    inputUrl = getProperties().getProperty(REDSHIFT_GCS_INPUT_URL);
    inputTable = getProperties().getProperty(REDSHIFT_GCS_INPUT_TABLE);
    tempDir = getProperties().getProperty(REDSHIFT_GCS_TEMP_DIR);
    iamRole = getProperties().getProperty(REDSHIFT_GCS_IAM_ROLE);
    accessKey = getProperties().getProperty(REDSHIFT_GCS_ACCESS_KEY);
    secretKey = getProperties().getProperty(REDSHIFT_GCS_SECRET_KEY);
    fileFormat = getProperties().getProperty(REDSHIFT_GCS_FILE_FORMAT);
    fileLocation = getProperties().getProperty(REDSHIFT_GCS_FILE_LOCATION);
    outputMode = getProperties().getProperty(REDSHIFT_GCS_OUTPUT_MODE);
  }

  @Override
  public void runTemplate() {

    validateInput();

    SparkSession spark = SparkSession.builder().appName("Spark HiveToGcs Job").getOrCreate();
    LOGGER.info("RedshiftToGcs job started.");

    spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", accessKey);
    spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", secretKey);

    Dataset<Row> inputData =
        spark
            .read()
            .format("io.github.spark_redshift_community.spark.redshift")
            .option("url", inputUrl)
            .option("dbtable", inputTable)
            .option("tempdir", tempDir)
            .option("aws_iam_role", iamRole)
            .load();

    DataFrameWriter<Row> writer = inputData.write().format(fileFormat);
    writer.mode(outputMode).save(fileLocation);

    LOGGER.info("RedshiftToGcs job completed.");
    spark.stop();
  }

  void validateInput() {
    if (StringUtils.isAllBlank(inputUrl)
            || StringUtils.isAllBlank(inputTable)
            || StringUtils.isAllBlank(tempDir)
            || StringUtils.isAllBlank(iamRole)
            || StringUtils.isAllBlank(accessKey)
            || StringUtils.isAllBlank(secretKey)
            || StringUtils.isAllBlank(fileFormat)
            || StringUtils.isAllBlank(fileLocation)) {
      LOGGER.error(
              "{},{},{},{},{},{},{},{} is required parameter. ",
              REDSHIFT_GCS_INPUT_URL,
              REDSHIFT_GCS_INPUT_TABLE,
              REDSHIFT_GCS_TEMP_DIR,
              REDSHIFT_GCS_IAM_ROLE,
              REDSHIFT_GCS_ACCESS_KEY,
              REDSHIFT_GCS_SECRET_KEY,
              REDSHIFT_GCS_FILE_FORMAT,
              REDSHIFT_GCS_FILE_LOCATION);
      throw new IllegalArgumentException(
              "Required parameters for RedshiftToGCS not passed. "
                      + "Set mandatory parameter for RedshiftToGCS template "
                      + "in resources/conf/template.properties file.");
    }

    LOGGER.info(
            "Starting Hive to GCS spark job with following parameters:"
                    + "1. {}:{}"
                    + "2. {}:{}"
                    + "3. {}:{}"
                    + "4. {},{}"
                    + "5. {},{}"
                    + "6. {},{}"
                    + "7. {},{}"
                    + "8. {},{}",
            REDSHIFT_GCS_INPUT_URL,
            inputUrl,
            REDSHIFT_GCS_INPUT_TABLE,
            inputTable,
            REDSHIFT_GCS_TEMP_DIR,
            tempDir,
            REDSHIFT_GCS_IAM_ROLE,
            iamRole,
            REDSHIFT_GCS_ACCESS_KEY,
            accessKey,
            REDSHIFT_GCS_SECRET_KEY,
            secretKey,
            REDSHIFT_GCS_FILE_FORMAT,
            fileFormat,
            REDSHIFT_GCS_FILE_LOCATION,
            fileLocation);
  }
}
