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

import com.datastax.spark.connector.CassandraSparkExtensions;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.TemplateConstants;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraToGCS implements BaseTemplate, TemplateConstants {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraToGCS.class);
  private final CassandraToGCSConfig config;

  public CassandraToGCS(CassandraToGCSConfig config) {
    this.config = config;
  }

  public static CassandraToGCS of(String... args) {
    CassandraToGCSConfig config = CassandraToGCSConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new CassandraToGCS(config);
  }

  @Override
  public void runTemplate() throws StreamingQueryException, TimeoutException {
    validateInput();
    Dataset dataset;
    SparkSession spark =
        SparkSession.builder()
            .appName("Spark CassandraToGCS Job")
            .withExtensions(new CassandraSparkExtensions())
            .config(
                "spark.sql.catalog." + config.getCatalog(),
                "com.datastax.spark.connector.datasource.CassandraCatalog")
            .config(
                "spark.sql.catalog." + config.getCatalog() + ".spark.cassandra.connection.host",
                config.getHost())
            .getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(config.getSparkLogLevel());

    if (StringUtils.isAllBlank(config.getQuery())) {
      dataset =
          spark.sql(
              String.format(
                  "SELECT * FROM %1$s.%2$s.%3$s",
                  config.getCatalog(), config.getKeyspace(), config.getInputTable()));
    } else {
      dataset = spark.sql(config.getQuery());
    }
    dataset
        .write()
        .format(config.getOutputFormat())
        .mode(config.getSaveMode())
        .save(config.getOutputpath());
  }

  public void validateInput() {}
}
