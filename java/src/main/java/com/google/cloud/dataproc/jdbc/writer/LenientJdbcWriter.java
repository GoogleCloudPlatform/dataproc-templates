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

import static org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createTable;
import static org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.dropTable;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.immutable.Map;

public class LenientJdbcWriter implements CreatableRelationProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(LenientJdbcWriter.class);

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> df) {
    JdbcOptionsInWrite options = new JdbcOptionsInWrite(parameters);
    boolean isCaseSensitive = SparkSession.active().sessionState().conf().caseSensitiveAnalysis();
    JdbcDialect dialect = JdbcDialects.get(options.url());
    Connection conn = dialect.createConnectionFactory(options).apply(-1);
    try {
      boolean tableExists = JdbcUtils.tableExists(conn, options);
      if (tableExists) {
        switch (mode) {
          case Overwrite:
            dropTable(conn, options.table(), options);
            createTable(conn, options.table(), df.schema(), isCaseSensitive, options);
            Option<StructType> schema = scala.Option.apply(df.schema());
            saveTableLenient(df, options, isCaseSensitive, schema);
            break;

          case Append:
            Option<StructType> tableSchema = JdbcUtils.getSchemaOption(conn, options);
            saveTableLenient(df, options, isCaseSensitive, tableSchema);
            break;

          case ErrorIfExists:
            throw new RuntimeException(
                String.format(
                    "Table or view '%s' already exists. SaveMode: ErrorIfExists.",
                    options.table()));

          case Ignore:
            // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
            // to not save the contents of the DataFrame and to not change the existing data.
            // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        createTable(conn, options.table(), df.schema(), isCaseSensitive, options);
        Option<StructType> schema = Option.apply(df.schema());
        saveTableLenient(df, options, isCaseSensitive, schema);
      }
    } finally {
      try {
        conn.close();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    return new BaseRelation() {
      @Override
      public SQLContext sqlContext() {
        return sqlContext;
      }

      @Override
      public StructType schema() {
        return df.schema();
      }
    };
  }

  private void saveTableLenient(
      Dataset<Row> df,
      JdbcOptionsInWrite options,
      boolean isCaseSensitive,
      Option<StructType> tableSchema) {
    String url = options.url();
    String table = options.table();
    JdbcDialect dialect = JdbcDialects.get(url);
    StructType rddSchema = df.schema();
    int batchSize = options.batchSize();
    int isolationLevel = options.isolationLevel();

    String insertStmt =
        JdbcUtils.getInsertStatement(table, rddSchema, tableSchema, isCaseSensitive, dialect);

    Integer numPartitions = options.numPartitions().getOrElse(() -> 1);

    Dataset<Row> repartitionedDF = df;

    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid value `%d` for parameter `%s` in table writing "
                  + "via JDBC. The minimum value is 1.",
              numPartitions, JDBCOptions.JDBC_NUM_PARTITIONS()));

    } else if (numPartitions < df.rdd().getNumPartitions())
      repartitionedDF = df.coalesce(numPartitions);

    repartitionedDF
        .javaRDD()
        .foreachPartition(
            new SavePartitionLenient(
                table, rddSchema, insertStmt, batchSize, dialect, isolationLevel, options));
  }
}
