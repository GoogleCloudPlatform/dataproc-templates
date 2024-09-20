/*
 * Copyright (C) 2024 Google LLC
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

import static org.apache.spark.sql.types.DataTypes.*;

import java.sql.Types;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MetadataBuilder;
import scala.Option;

public class SpannerPostgresJDBCDialect extends JdbcDialect {

  @Override
  public boolean canHandle(String url) {
    return url.toLowerCase().startsWith("jdbc:postgresql:");
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
      return Option.apply(new JdbcType("INT", 4));
    } else if (LongType.equals(dt)) {
      return Option.apply(new JdbcType("INT8", -5));
    } else if (DoubleType.equals(dt)) {
      return Option.apply(new JdbcType("FLOAT8", 8));
    } else if (FloatType.equals(dt)) {
      return Option.apply(new JdbcType("FLOAT8", 6));
    } else if (ShortType.equals(dt)) {
      return Option.apply(new JdbcType("INT8", 5));
    } else if (ByteType.equals(dt)) {
      return Option.apply(new JdbcType("BYTEA", 2004));
    } else if (BooleanType.equals(dt)) {
      return Option.apply(new JdbcType("BOOL", -7));
    } else if (StringType.equals(dt)) {
      return Option.apply(new JdbcType("TEXT", 2005));
    } else if (BinaryType.equals(dt)) {
      org.apache.spark.sql.types.BinaryType binaryType = (org.apache.spark.sql.types.BinaryType) dt;
      return Option.apply(new JdbcType("BYTEA", 2004));
    } else if (TimestampType.equals(dt)) {
      return Option.apply(new JdbcType("TIMESTAMPTZ", 93));
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
    if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL)
      return Option.apply(DecimalType.apply(38, 9));
    return Option.empty();
  }
}
