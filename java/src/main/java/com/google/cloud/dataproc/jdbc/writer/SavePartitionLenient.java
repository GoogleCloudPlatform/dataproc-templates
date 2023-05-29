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
package com.google.cloud.dataproc.jdbc.writer;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

public class SavePartitionLenient implements VoidFunction<Iterator<Row>>, Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SavePartitionLenient.class);

  String table;
  StructType rddSchema;
  String insertStmt;
  int batchSize;
  JdbcDialect dialect;
  int isolationLevel;
  JdbcOptionsInWrite options;

  public SavePartitionLenient(
      String table,
      StructType rddSchema,
      String insertStmt,
      int batchSize,
      JdbcDialect dialect,
      int isolationLevel,
      JdbcOptionsInWrite options) {
    this.table = table;
    this.rddSchema = rddSchema;
    this.insertStmt = insertStmt;
    this.batchSize = batchSize;
    this.dialect = dialect;
    this.isolationLevel = isolationLevel;
    this.options = options;
  }

  @Override
  public void call(Iterator<Row> rowIterator) {
    scala.collection.Iterator<Row> scalaRows = JavaConverters.asScalaIterator(rowIterator);

    try {
      // rowIterator.
      LOGGER.info(
          String.format(
              "############### saving partition of size: %d ***************", scalaRows.size()));
      JdbcUtils.savePartition(
          table, scalaRows, rddSchema, insertStmt, batchSize, dialect, isolationLevel, options);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
