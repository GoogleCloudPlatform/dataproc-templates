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

/**
 * SavePartitionLenient class implements Spark's VoidFunction, designed to write
 * a partition of rows from an RDD (Resilient Distributed Dataset) to a JDBC table
 * with lenient handling. It writes each partition of data in batches.
 */
public class SavePartitionLenient implements VoidFunction<Iterator<Row>>, Serializable {

  // Logger to capture and log events and messages
  private static final Logger LOGGER = LoggerFactory.getLogger(SavePartitionLenient.class);

  // Define class properties for table details and JDBC options
  String table; // The name of the table to write data to
  StructType rddSchema; // The schema of the RDD (DataFrame)
  String insertStmt; // SQL insert statement for batch writing
  int batchSize; // Size of the batch to insert at a time
  JdbcDialect dialect; // The JDBC dialect for the target database
  int isolationLevel; // Transaction isolation level for JDBC operations
  JdbcOptionsInWrite options; // JDBC options containing connection and table details

  /**
   * Constructor to initialize the SavePartitionLenient class with the required properties.
   * @param table The name of the table to insert data into.
   * @param rddSchema The schema of the DataFrame/RDD being written.
   * @param insertStmt The SQL insert statement to be used for writing.
   * @param batchSize The number of rows to write in each batch.
   * @param dialect The JDBC dialect for handling database-specific operations.
   * @param isolationLevel The transaction isolation level.
   * @param options The options containing JDBC connection and configuration details.
   */
  public SavePartitionLenient(
      String table,
      StructType rddSchema,
      String insertStmt,
      int batchSize,
      JdbcDialect dialect,
      int isolationLevel,
      JdbcOptionsInWrite options) {
    // Initialize class variables
    this.table = table;
    this.rddSchema = rddSchema;
    this.insertStmt = insertStmt;
    this.batchSize = batchSize;
    this.dialect = dialect;
    this.isolationLevel = isolationLevel;
    this.options = options;
  }

  /**
   * Override the call method to save each partition of the DataFrame to the JDBC table.
   * This method is executed for each partition of the RDD in the Spark job.
   * @param rowIterator The iterator over rows in the current partition.
   */
  @Override
  public void call(Iterator<Row> rowIterator) {
    // Convert the Java row iterator to a Scala iterator for compatibility with Scala APIs
    scala.collection.Iterator<Row> scalaRows = JavaConverters.asScalaIterator(rowIterator);

    try {
      // Use JdbcUtils to save the partition of rows to the JDBC table in batches
      JdbcUtils.savePartition(
          table, scalaRows, rddSchema, insertStmt, batchSize, dialect, isolationLevel, options);
    } catch (Exception e) {
      // Log and print the exception if any error occurs during the save operation
      e.printStackTrace();
    }
  }
}