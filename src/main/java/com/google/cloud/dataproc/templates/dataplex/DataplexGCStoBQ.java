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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.gcs.GCStoBigquery;
import com.google.cloud.dataproc.templates.util.DataplexUtil;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.cli.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataplexGCStoBQ implements BaseTemplate {

  public static final Logger LOGGER = LoggerFactory.getLogger(GCStoBigquery.class);
  public static String CUSTOM_SQL_GCS_PATH_OPTION = "custom_sql_gcs_path";

  private SparkSession spark;
  private SQLContext sqlContext;
  private String projectId;
  private String customSqlGCSPath;
  private String entityDataBasePath;
  private String entity;
  private String materializationDataset;
  private String targetTable;
  private String inputFileFormat;
  private String bqTempBucket;

  public DataplexGCStoBQ(String pCustomSqlGCSPath) {
    customSqlGCSPath = pCustomSqlGCSPath;
    projectId = getProperties().getProperty(PROJECT_ID_PROP);
    entity = getProperties().getProperty(DATAPLEX_GCS_BQ_ENTITY);
    materializationDataset = getProperties().getProperty(DATAPLEX_GCS_BQ_MATERIALIZATION_DATASET);
    targetTable = getProperties().getProperty(DATAPLEX_GCS_BQ_TARGET_TABLE);
    bqTempBucket = getProperties().getProperty(GCS_BQ_LD_TEMP_BUCKET_NAME);
  }

  public static DataplexGCStoBQ of(String... args) {
    CommandLine cmd = parseArguments(args);
    String customSqlGCSPath = cmd.getOptionValue(CUSTOM_SQL_GCS_PATH_OPTION);
    return new DataplexGCStoBQ(customSqlGCSPath);
  }

  /**
   * Parse command line arguments to supply GCS path of file with custom sql
   *
   * @param args line arguments to supply template configuration yaml file at startup
   * @return parsed arguments
   */
  public static CommandLine parseArguments(String... args) {
    Options options = new Options();
    Option configFileOption =
        new Option(CUSTOM_SQL_GCS_PATH_OPTION, "gcs path of file containing custom sql");
    configFileOption.setRequired(false);
    configFileOption.setArgs(1);
    options.addOption(configFileOption);

    CommandLineParser parser = new BasicParser();
    try {
      return parser.parse(options, args);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Parse partitionsListWithLocationAndKeys into a Dataset partition path and key values for each
   * partition in Dataplex Entity
   *
   * @param partitionsListWithLocationAndKeys list with partition path and key values for each
   *     partition in Dataplex Entity
   * @param partitionKeysList list of partition keys for Dataplex Entity
   * @return a dataset with partition path and key values for each partition in Dataplex Entity
   */
  public Dataset<Row> getAllPartitionsDf(
      List<String> partitionsListWithLocationAndKeys, List<String> partitionKeysList) {
    Dataset allPartitionsDf =
        sqlContext.createDataset(partitionsListWithLocationAndKeys, Encoders.STRING()).toDF();
    allPartitionsDf =
        allPartitionsDf.selectExpr("*", "split(value, ',')[0] as __gcs_location_path__");
    for (int i = 0; i < partitionKeysList.size(); i += 1) {
      allPartitionsDf =
          allPartitionsDf.selectExpr(
              "*", String.format("split(value, ',')[%d] as %s", i + 1, partitionKeysList.get(i)));
    }
    allPartitionsDf = allPartitionsDf.drop("value");
    return allPartitionsDf;
  }

  /**
   * Query BQ target all distinct value of partition keys
   *
   * @param partitionKeysList list of partition keys for Dataplex Entity
   * @return a dataset with all distinct value of partition keys in BQ target table
   */
  private Dataset<Row> getBQTargetAvailablePartitionsDf(List<String> partitionKeysList) {
    spark.conf().set("viewsEnabled", "true");
    spark.conf().set("materializationDataset", materializationDataset);
    try {
      String sql =
          String.format(
              "select distinct %s FROM `%s`", String.join(",", partitionKeysList), targetTable);
      return spark.read().format("bigquery").load(sql);
    } catch (BigQueryConnectorException e) {
      if (e.getCause().toString().contains("Not found: Table")) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * Compare partitions in dataplex (partitionValuesInDataplex) with data available at BQ target
   * table (bqTargetAvailablePartitionsDf) to identify new partitions
   *
   * @param partitionKeysList list of partition keys for Dataplex Entity
   * @return a dataset with the GCS paths of new partitions
   */
  private Dataset<Row> getNewPartitionsPathsDS(
      List<String> partitionKeysList,
      Dataset<Row> dataplexPartitionsKeysDS,
      Dataset<Row> bqPartitionsKeysDS) {
    if (bqPartitionsKeysDS == null) {
      return dataplexPartitionsKeysDS.select("__gcs_location_path__");
    }

    dataplexPartitionsKeysDS.createOrReplaceTempView("dataplexPartitionsKeysDS");
    bqPartitionsKeysDS.createOrReplaceTempView("bqPartitionsKeysDS");
    String joinClause =
        partitionKeysList.stream()
            .map(str -> String.format("t1.%s=t2.%s", str, str))
            .collect(Collectors.joining(" AND "));
    Dataset<Row> newPartitionsDS =
        spark.sql(
            "SELECT __gcs_location_path__ "
                + "FROM dataplexPartitionsKeysDS t1 "
                + "LEFT JOIN bqPartitionsKeysDS t2 ON "
                + joinClause
                + " WHERE t2.id is null");
    return newPartitionsDS;
  }

  /**
   * Loads from GCS all new partitions
   *
   * @param newPartitionsPathsDf a dataset with the GCS paths of new partitions
   * @return a dataset with the GCS paths of new partitions
   */
  private Dataset<Row> getNewPartitionsDS(Dataset<Row> newPartitionsPathsDf) {
    Row[] result = (Row[]) newPartitionsPathsDf.select("__gcs_location_path__").collect();
    Dataset<Row> newPartitionsDS = null;
    for (Row row : result) {
      Dataset<Row> newPartitionTempDS =
          sqlContext
              .read()
              .format(inputFileFormat)
              .option(GCS_BQ_CSV_HEADER, true)
              .option(GCS_BQ_CSV_INFOR_SCHEMA, true)
              .option(DATAPLEX_GCS_BQ_BASE_PATH_PROP_NAME, entityDataBasePath)
              .load(row.get(0).toString() + "/*");
      if (newPartitionsDS == null) {
        newPartitionsDS = newPartitionTempDS;
      } else {
        newPartitionsDS = newPartitionsDS.union(newPartitionTempDS);
      }
    }
    return newPartitionsDS;
  }

  /**
   * Load custom sql from GCS and apply SQL to output dataset
   *
   * @param newPartitionsDf dataset with new partitions
   * @return a dataset after custom sql has been applied
   * @throws IOException when Google Credential setup fails
   */
  public Dataset<Row> applyCustomSql(Dataset<Row> newPartitionsDf) throws IOException {
    if (customSqlGCSPath != null) {
      String[] pathAsList = customSqlGCSPath.replace("gs://", "").split("/");
      String BUCKET_NAME = pathAsList[0];
      String OBJECT_NAME = Stream.of(pathAsList).skip(1).collect(Collectors.joining("/"));
      System.out.println(BUCKET_NAME);
      System.out.println(OBJECT_NAME);
      StorageOptions options =
          StorageOptions.newBuilder()
              .setProjectId(projectId)
              .setCredentials(GoogleCredentials.getApplicationDefault())
              .build();

      Storage storage = options.getService();
      Blob blob = storage.get(BUCKET_NAME, OBJECT_NAME);
      String custom_sql = new String(blob.getContent());
      newPartitionsDf.createOrReplaceTempView("__table__");
      return spark.sql(custom_sql);
    } else {
      return newPartitionsDf;
    }
  }

  /**
   * Loads dataset to BigQuery
   *
   * @param newPartitionsDf dataset with new partitions
   */
  public void writeToBQ(Dataset<Row> newPartitionsDf) {
    if (newPartitionsDf != null) {
      newPartitionsDf
          .write()
          .format(GCS_BQ_OUTPUT_FORMAT)
          .option(GCS_BQ_OUTPUT, targetTable)
          .option(GCS_BQ_TEMP_BUCKET, bqTempBucket)
          .option(
              DATAPLEX_GCS_BQ_CREATE_DISPOSITION_PROP_NAME,
              DATAPLEX_GCS_BQ_CREATE_DISPOSITION_CREATE_IF_NEEDED)
          .mode(SaveMode.Append)
          .save();
    }
  }

  public void runTemplate() {
    try {
      this.spark = SparkSession.builder().appName("Dataplex GCS to BQ").getOrCreate();
      this.sqlContext = new SQLContext(spark);
      this.entityDataBasePath = DataplexUtil.getEntityDataBasePath(entity);
      this.inputFileFormat = DataplexUtil.getInputFileFormat(entity);

      List<String> partitionKeysList = DataplexUtil.getPartitionKeyList(entity);
      List<String> partitionsListWithLocationAndKeys =
          DataplexUtil.getPartitionsListWithLocationAndKeys(entity);

      // Building dataset with all partitions keys in Dataplex Entity
      Dataset<Row> dataplexPartitionsKeysDS =
          getAllPartitionsDf(partitionsListWithLocationAndKeys, partitionKeysList);

      // Querying BQ for all partition keys currently present in target table
      Dataset<Row> bqPartitionsKeysDS = getBQTargetAvailablePartitionsDf(partitionKeysList);

      // Compare dataplexPartitionsKeysDS and bqPartitionsKeysDS to indetify new
      // partitions
      Dataset<Row> newPartitionsPathsDS =
          getNewPartitionsPathsDS(partitionKeysList, dataplexPartitionsKeysDS, bqPartitionsKeysDS);

      // load data from each partition
      Dataset<Row> newPartitionsDS = getNewPartitionsDS(newPartitionsPathsDS);

      newPartitionsDS = applyCustomSql(newPartitionsDS);

      newPartitionsDS = DataplexUtil.castDatasetToDataplexSchema(newPartitionsDS, entity);

      writeToBQ(newPartitionsDS);

    } catch (Throwable th) {
      LOGGER.error("Exception in DataplexGCStoBQ", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
