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
package com.google.cloud.dataproc.templates.kafka;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_BOOTSTRAP_SERVERS;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_GCS_OUTPUT_LOCATION;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_MESSAGE_FORMAT;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_SCHEMA_URL;
import static com.google.cloud.dataproc.templates.util.TemplateConstants.KAFKA_TOPIC;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class KafkaReaderTest {

  private KafkaReader reader;
  private static SparkSession spark;
  private static Dataset<Row> jsonData;
  private static Dataset<Row> bytesData;
  private StructType messageSchema;

  @BeforeAll
  static void setUp() throws IOException {
    PropertyUtil.getProperties().setProperty(KAFKA_GCS_OUTPUT_LOCATION, "gs://test-bucket");
    PropertyUtil.getProperties().setProperty(KAFKA_BOOTSTRAP_SERVERS, "localhost:5001");
    PropertyUtil.getProperties().setProperty(KAFKA_TOPIC, "test");
    PropertyUtil.getProperties().setProperty(KAFKA_MESSAGE_FORMAT, "json");
    PropertyUtil.getProperties().setProperty(KAFKA_SCHEMA_URL, "");

    // creating s
    spark = SparkSession.builder().master("local").getOrCreate();

    // Mock Json message
    List<Row> jsonMessage = Arrays.asList(RowFactory.create("key1", "{'id':'001','val':'One'}}"));

    // Mock CSV message
    List<Row> bytesMessage = Arrays.asList(RowFactory.create("key", "value"));

    // Dataframe Schema
    List<org.apache.spark.sql.types.StructField> listOfStructField =
        Arrays.asList(
            DataTypes.createStructField("key", DataTypes.StringType, true),
            DataTypes.createStructField("value", DataTypes.StringType, true));
    StructType schema = DataTypes.createStructType(listOfStructField);

    jsonData = spark.createDataFrame(jsonMessage, schema);
    bytesData = spark.createDataFrame(bytesMessage, schema);
  }

  @Test
  void runProcessJsonMessageWithValidParameters() {
    // Mock message schema
    messageSchema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("val", DataTypes.StringType, true)));

    reader = new KafkaReader();
    assertEquals(
        org.apache.spark.sql.Dataset.class,
        reader.processJsonMessage(jsonData, messageSchema).getClass());

    assertEquals(
        "001",
        reader
            .processJsonMessage(jsonData, messageSchema)
            .select("id")
            .collectAsList()
            .get(0)
            .get(0)
            .toString());
  }

  @Test
  void runProcessJsonMessageWithoutSchema() {
    reader = new KafkaReader();
    // No meesage schema found
    messageSchema = null;
    assertThrows(
        NullPointerException.class, () -> reader.processJsonMessage(jsonData, messageSchema));
  }

  @Test
  void runProcessBytestMessageWithValidParameter() {
    reader = new KafkaReader();
    messageSchema = null;
    assertEquals(
        org.apache.spark.sql.Dataset.class, reader.processBytesMessage(bytesData).getClass());

    assertEquals(
        "value",
        reader
            .processBytesMessage(bytesData)
            .select("value")
            .collectAsList()
            .get(0)
            .get(0)
            .toString());
  }

  @AfterAll
  static void tearDown() {
    spark.stop();
  }
}
