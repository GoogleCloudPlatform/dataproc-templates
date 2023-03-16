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
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraToBQ implements BaseTemplate {
  private CassandraToBqConfig config;
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraToBQ.class);

  public CassandraToBQ(CassandraToBqConfig config) {
    this.config = config;
  }

  public static CassandraToBQ of(String... args) {
    CassandraToBqConfig config = CassandraToBqConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new CassandraToBQ(config);
  }

  @Override
  public void runTemplate() throws StreamingQueryException, TimeoutException {
    validateInput();
    Dataset dataset;
    SparkSession spark =
        SparkSession.builder()
            .appName("Spark CassandraToBQ Job")
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
        .mode(config.getMode())
        .format("com.google.cloud.spark.bigquery")
        .option("table", config.getBqLocation())
        .option("temporaryGcsBucket", config.getTemplocation())
        .save();
  }

  public void validateInput() {}
}
