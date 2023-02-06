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
package com.google.cloud.dataproc.templates.dataplex;

import static com.google.cloud.dataproc.templates.util.TemplateConstants.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.cloud.dataproc.templates.util.PropertyUtil;
import java.util.Properties;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import com.google.auth.oauth2.GoogleCredentials;
// import com.google.cloud.dataproc.templates.BaseTemplate;
// import com.google.cloud.dataproc.templates.gcs.GCStoBigquery;
// import com.google.cloud.dataproc.templates.util.Dataplex.DataplexAssetUtil;
// import com.google.cloud.dataproc.templates.util.Dataplex.DataplexEntityUtil;
// import com.google.cloud.dataproc.templates.util.DataprocTemplateException;
// import
// com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
// import com.google.cloud.storage.Blob;
// import com.google.cloud.storage.Storage;
// import com.google.cloud.storage.StorageOptions;
// import java.io.IOException;
// import java.util.List;
// import java.util.Objects;
// import java.util.stream.Collectors;
// import java.util.stream.Stream;
// import com.google.cloud.dataproc.templates.util.PropertyUtil;

// import org.apache.commons.cli.*;
// import org.apache.commons.lang3.StringUtils;
// import org.apache.spark.sql.DataFrameReader;
// import org.apache.spark.sql.DataFrameWriter;
// import org.apache.spark.sql.Dataset;
// import org.apache.spark.sql.Encoders;
// import org.apache.spark.sql.Row;
// import org.apache.spark.sql.SQLContext;
// import org.apache.spark.sql.SaveMode;

public class DataplexGCStoBQTest {

  private DataplexGCStoBQ dataplexGCStoBQTest;

  private static final Logger LOGGER = LoggerFactory.getLogger(DataplexGCStoBQTest.class);

  @BeforeEach
  void setUp() {
    System.setProperty("hadoop.home.dir", "/");
    SparkSession spark = SparkSession.builder().master("local").getOrCreate();
  }

  @Test
  void runTemplateWithValidParameters() {
    LOGGER.info("Running test: runTemplateWithValidParameters");
    Properties props = PropertyUtil.getProperties();
    props.setProperty(
        PROJECT_ID_PROP,
        "projectID"); // if not given it should throw an error, it does not right now
    props.setProperty(DATAPLEX_GCS_BQ_TARGET_DATASET, "gs://test-bucket");
    props.setProperty(DATAPLEX_GCS_BQ_TARGET_ASSET, "bigqueryDataset");
    props.setProperty(DATAPLEX_GCS_BQ_TARGET_ENTITY, "bigqueryTable");
    props.setProperty(DATAPLEX_GCS_BQ_SAVE_MODE, "parquet");
    props.setProperty(DATAPLEX_GCS_BQ_INCREMENTAL_PARTITION_COPY, "parquet");
    props.setProperty(DATAPLEX_GCS_BQ_INCREMENTAL_PARTITION_COPY, "parquet");
    props.setProperty(DATAPLEX_GCS_BQ_INCREMENTAL_PARTITION_COPY, "parquet");
    props.setProperty(GCS_BQ_LD_TEMP_BUCKET_NAME, "gs://temp-bucket");
    dataplexGCStoBQTest =
        new DataplexGCStoBQ(
            null,
            "projects/yadavaja-sandbox/locations/us-west1/lakes/dataproc-templates-test-lake/zones/dataplex-gcs-to-bq/entities/dataplex_gcs",
            null,
            null,
            "projects/yadavaja-sandbox/locations/us-west1/lakes/dataproc-templates-test-lake/zones/dataplex-gcs-to-bq/entities/dataplex-gcs-to-bq");

    assertDoesNotThrow(dataplexGCStoBQTest::validateInput);
  }

  //     @ParameterizedTest
  //     @MethodSource("propertyKeys")
  //     void runTemplateWithInvalidParameters(String propKey) {
  //         LOGGER.info("Running test: runTemplateWithInvalidParameters");
  //         PropertyUtil.getProperties().setProperty(propKey, "");
  //         gcsCsvToBiqueryTest = new GCStoBigquery();
  //         Exception exception =
  //             assertThrows(IllegalArgumentException.class, () ->
  // gcsCsvToBiqueryTest.runTemplate());
  //         assertEquals(
  //             "Required parameters for GCStoBQ not passed. "
  //                 + "Set mandatory parameter for GCStoBQ template"
  //                 + " in resources/conf/template.properties file.",
  //             exception.getMessage());
  //     }

  //     static Stream<String> propertyKeys() {
  //         return Stream.of(
  //             PROJECT_ID_PROP,
  //             GCS_BQ_INPUT_LOCATION,
  //             GCS_OUTPUT_DATASET_NAME,
  //             GCS_OUTPUT_TABLE_NAME,
  //             GCS_BQ_INPUT_FORMAT);
  //     }
}

