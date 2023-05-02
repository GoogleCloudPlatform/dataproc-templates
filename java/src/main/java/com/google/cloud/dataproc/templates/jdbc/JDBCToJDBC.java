/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.dataproc.templates.jdbc;

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.util.PropertyUtil;
import com.google.cloud.dataproc.templates.util.ValidationUtil;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToJDBC implements BaseTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCToJDBC.class);
  private final JDBCToJDBCConfig config;

  public JDBCToJDBC(JDBCToJDBCConfig config) {
    this.config = config;
  }

  public static JDBCToJDBC of(String... args) {
    JDBCToJDBCConfig config = JDBCToJDBCConfig.fromProperties(PropertyUtil.getProperties());
    LOGGER.info("Config loaded\n{}", config);
    return new JDBCToJDBC(config);
  }

  @Override
  public void validateInput() {
    ValidationUtil.validateOrThrow(config);
  }

  @Override
  public void runTemplate() throws SQLException {
    SparkSession spark = SparkSession.builder().appName("JDBC to JDBC").getOrCreate();
    // Set log level
    spark.sparkContext().setLogLevel(config.getSparkLogLevel());
    Dataset<Row> inputData = load(spark);
    write(inputData);

    if (StringUtils.isNotBlank(config.getJdbcOutputPrimaryKey())) {
      addPrimaryKeyColumn();
    }
  }

  public Dataset<Row> load(SparkSession spark) {
    HashMap<String, String> jdbcProperties = config.getJDBCProperties();
    Dataset<Row> inputData = spark.read().format("jdbc").options(jdbcProperties).load();

    if (StringUtils.isNotBlank(config.getJdbcTempSQLQuery())) {
      inputData.createOrReplaceGlobalTempView(config.getJdbcTempView());
      inputData = spark.sql(config.getJdbcTempSQLQuery());
    }

    return inputData;
  }

  public void write(Dataset<Row> inputData) {
    if (StringUtils.isNotBlank(config.getJdbcNumPartitions())) {
      inputData
          .write()
          .format("jdbc")
          .option(JDBCOptions.JDBC_URL(), config.getJdbcOutputURL())
          .option(JDBCOptions.JDBC_TABLE_NAME(), config.getJdbcOutputTable())
          .option(JDBCOptions.JDBC_DRIVER_CLASS(), config.getJdbcOutputDriver())
          .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), config.getJdbcOutputBatchSize())
          .option(JDBCOptions.JDBC_CREATE_TABLE_OPTIONS(), config.getJdbcOutputCreateTableOption())
          .option(JDBCOptions.JDBC_NUM_PARTITIONS(), config.getJdbcNumPartitions())
          .mode(config.getJdbcOutputMode())
          .save();
    } else {
      inputData
          .write()
          .format("jdbc")
          .option(JDBCOptions.JDBC_URL(), config.getJdbcOutputURL())
          .option(JDBCOptions.JDBC_TABLE_NAME(), config.getJdbcOutputTable())
          .option(JDBCOptions.JDBC_DRIVER_CLASS(), config.getJdbcOutputDriver())
          .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE(), config.getJdbcOutputBatchSize())
          .option(JDBCOptions.JDBC_CREATE_TABLE_OPTIONS(), config.getJdbcOutputCreateTableOption())
          .mode(config.getJdbcOutputMode())
          .save();
    }
  }

  public void addPrimaryKeyColumn() throws SQLException {
    Connection connection = DriverManager.getConnection(config.getJdbcOutputURL());
    Statement statement = connection.createStatement();

    statement.executeUpdate(
        String.format(
            "ALTER TABLE %s ADD PRIMARY KEY (%s)",
            config.getJdbcOutputTable(), config.getJdbcOutputPrimaryKey()));

    statement.close();
    connection.close();
  }
}
