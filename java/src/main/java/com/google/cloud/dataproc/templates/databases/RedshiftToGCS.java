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
package com.google.cloud.dataproc.templates.databases;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedshiftToGCS implements BaseTemplate {

  public static final Logger LOGGER =
      LoggerFactory.getLogger(com.google.cloud.dataproc.templates.databases.RedshiftToGCS.class);
  private final RedshiftToGCSConfig config;

  public RedshiftToGCS(RedshiftToGCSConfig config) {
    this.config = config;
  }

  public static com.google.cloud.dataproc.templates.databases.RedshiftToGCS of(String... args) {
    RedshiftToGCSConfig config = RedshiftToGCSConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new com.google.cloud.dataproc.templates.databases.RedshiftToGCS(config);
  }

  @Override
  public void runTemplate() {
    validateInput();
    SparkSession spark =
        SparkSession.builder()
            .appName("Spark Template RedshiftToGCS ")
            .enableHiveSupport()
            .getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(config.getSparkLogLevel());

    LOGGER.info("RedshiftToGcs job started.");

    spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", config.getAWSAccessKey());
    spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", config.getAWSSecretKey());

    Dataset<Row> inputData =
        spark
            .read()
            .format("io.github.spark_redshift_community.spark.redshift")
            .option("url", config.getAWSURL())
            .option("dbtable", config.getAWSTable())
            .option("tempdir", config.getAWSDir())
            .option("aws_iam_role", config.getAWSRole())
            .load();

    if (StringUtils.isNotBlank(config.gettempTable())
        && StringUtils.isNotBlank(config.gettempQuery())) {
      inputData.createOrReplaceGlobalTempView(config.gettempTable());
      inputData = spark.sql(config.gettempQuery());
    }

    DataFrameWriter<Row> writer =
        inputData.write().mode(config.getGcsWriteMode()).format(config.getGcsOutputFormat());

    writer.save(config.getGcsOutputLocation());

    LOGGER.info("RedshiftToGcs job completed.");
    spark.stop();
  }

  public void validateInput() {}
}
