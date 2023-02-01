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
package com.google.cloud.dataproc.templates.databases;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;

import com.google.cloud.dataproc.dialects.SpannerJdbcDialect;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.util.HashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerToGCS implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerToGCS.class);
  public static final String SPANNER_JDBC_DRIVER = "com.google.cloud.spanner.jdbc.JdbcDriver";

  private final SpannerToGCSConfig config;

  public SpannerToGCS(SpannerToGCSConfig config) {
    this.config = config;
  }

  public static SpannerToGCS of(String... args) {
    SpannerToGCSConfig config = SpannerToGCSConfig.fromProperties(PropertyUtil.getProperties());
    ValidationUtil.validateOrThrow(config);
    LOGGER.info("Config loaded\n{}", config);
    return new SpannerToGCS(config);
  }

  @Override
  public void runTemplate() {
    validateInput();
    JdbcDialects.registerDialect(new SpannerJdbcDialect());

    SparkSession spark = SparkSession.builder().appName("DatabaseToGCS Dataproc job").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(config.getSparkLogLevel());

    LOGGER.debug("added jars : {}", spark.sparkContext().addedJars().keys());

    HashMap<String, String> jdbcProperties = new HashMap<>();
    jdbcProperties.put(JDBCOptions.JDBC_URL(), config.getSpannerJdbcUrl());
    jdbcProperties.put(JDBCOptions.JDBC_TABLE_NAME(), config.getInputTableId());
    jdbcProperties.put(JDBCOptions.JDBC_DRIVER_CLASS(), SPANNER_JDBC_DRIVER);

    if (StringUtils.isNotBlank(config.getConcatedPartitionProperties())) {
      jdbcProperties.put(JDBCOptions.JDBC_PARTITION_COLUMN(), config.getSqlPartitionColumn());
      jdbcProperties.put(JDBCOptions.JDBC_UPPER_BOUND(), config.getSqlUpperBound());
      jdbcProperties.put(JDBCOptions.JDBC_LOWER_BOUND(), config.getSqlLowerBound());
      jdbcProperties.put(JDBCOptions.JDBC_NUM_PARTITIONS(), config.getSqlNumPartitions());
    }

    Dataset<Row> jdbcDF = spark.read().format("jdbc").options(jdbcProperties).load();

    LOGGER.info("Data load complete from table/query: " + config.getInputTableId());

    if (StringUtils.isNotBlank(config.getTempTable())
        && StringUtils.isNotBlank(config.getTempQuery())) {
      jdbcDF.createOrReplaceGlobalTempView(config.getTempTable());
      jdbcDF = spark.sql(config.getTempQuery());
    }

    DataFrameWriter<Row> writer =
        jdbcDF.write().format(config.getGcsOutputFormat()).mode(config.getGcsWriteMode());

    writer.save(config.getGcsOutputLocation());

    spark.stop();
  }

  public void validateInput() {}
}
