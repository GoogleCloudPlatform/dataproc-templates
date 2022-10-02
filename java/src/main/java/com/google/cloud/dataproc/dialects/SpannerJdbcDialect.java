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
package com.google.cloud.dataproc.dialects;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.ByteType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.ShortType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

import java.sql.Types;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MetadataBuilder;
import scala.Option;

public class SpannerJdbcDialect extends JdbcDialect {

  private static final long serialVersionUID = 1L;

  @Override
  public boolean canHandle(String url) {
    return url.toLowerCase().startsWith("jdbc:cloudspanner:");
  }

  /**
   * Spanner uses backticks to quote column identifiers
   *
   * @param column column name
   * @return column name wrapped in backticks
   */
  @Override
  public String quoteIdentifier(String column) {
    return "`" + column + "`";
  }

  /**
   * Handle spanner DDL type names, used when creating tables when writing to spanner
   *
   * @param dt Spark SQL data type
   * @return Jdbc type representing a corresponding Spanner DDL type name
   */
  public Option<JdbcType> getJDBCType(final DataType dt) {
    if (IntegerType.equals(dt)) {
      return Option.apply(new JdbcType("INT64", 4));
    } else if (LongType.equals(dt)) {
      return Option.apply(new JdbcType("INT64", -5));
    } else if (DoubleType.equals(dt)) {
      return Option.apply(new JdbcType("FLOAT64", 8));
    } else if (FloatType.equals(dt)) {
      return Option.apply(new JdbcType("FLOAT64", 6));
    } else if (ShortType.equals(dt)) {
      return Option.apply(new JdbcType("INT64", 5));
    } else if (ByteType.equals(dt)) {
      return Option.apply(new JdbcType("BYTES(1)", -6));
    } else if (BooleanType.equals(dt)) {
      return Option.apply(new JdbcType("BOOL", -7));
    } else if (StringType.equals(dt)) {
      return Option.apply(new JdbcType("STRING(MAX)", 2005));
    } else if (BinaryType.equals(dt)) {
      org.apache.spark.sql.types.BinaryType binaryType = (org.apache.spark.sql.types.BinaryType) dt;
      return Option.apply(new JdbcType("BYTES(MAX)", 2004));
    } else if (TimestampType.equals(dt)) {
      return Option.apply(new JdbcType("TIMESTAMP", 93));
    } else if (DateType.equals(dt)) {
      return Option.apply(new JdbcType("DATE", 91));
    } else if (dt instanceof DecimalType) {
      return Option.apply(new JdbcType("NUMERIC", 3));
    } else {
      return Option.empty();
    }
  }

  @Override
  public Option<DataType> getCatalystType(
      int sqlType, String typeName, int size, MetadataBuilder md) {
    if (sqlType == Types.NUMERIC) return Option.apply(DecimalType.apply(38, 9));
    return Option.empty();
  }
}
