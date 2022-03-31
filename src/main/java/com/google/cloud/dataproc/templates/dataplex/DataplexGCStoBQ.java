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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.cli.*;
import org.apache.spark.sql.DataFrameWriter;
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
  public static String CUSTOM_SQL_GCS_PATH_OPTION = "customSqlGcsPath";
  public static String ENTITY_LIST_OPTION = "dataplexEntityList";
  public static String ASSET_LIST_OPTION = "dataplexAsset";
  public static String PARTITION_FIELD_OPTION = "partitionField";
  public static String PARTITION_TYPE_OPTION = "partitionType";
  public static String INCREMENTAL_PARTITION_COPY_NO = "no";

  private SparkSession spark;
  private SQLContext sqlContext;

  private String entitiesString;
  private String asset;
  private List<String> entityList;
  private String entity;
  private String partitionField;
  private String partitionType;

  private String projectId;
  private String customSqlGCSPath;
  private String entityBasePath;
  private String targetDataset;
  private String targetTableName;
  private String targetTable;
  private String inputFileFormat;
  private String bqTempBucket;
  private String sparkSaveMode;
  private String incrementalParittionCopy;

  public DataplexGCStoBQ(
      String customSqlGCSPath,
      String entitiesString,
      String asset,
      String partitionField,
      String partitionType) {
    this.customSqlGCSPath = customSqlGCSPath;
    this.entitiesString = entitiesString;
    this.asset = asset;
    this.partitionField = partitionField;
    this.partitionType = partitionType;

    projectId = getProperties().getProperty(PROJECT_ID_PROP);
    targetDataset = getProperties().getProperty(DATAPLEX_GCS_BQ_TARGET_DATASET);
    bqTempBucket = getProperties().getProperty(GCS_BQ_LD_TEMP_BUCKET_NAME);
    sparkSaveMode = getProperties().getProperty(DATAPLEX_GCS_BQ_SAVE_MODE);
    incrementalParittionCopy =
        getProperties().getProperty(DATAPLEX_GCS_BQ_INCREMENTAL_PARTITION_COPY);
    if (incrementalParittionCopy.equals(INCREMENTAL_PARTITION_COPY_NO)) {
      sparkSaveMode = SaveMode.Overwrite.toString();
    }
  }

  public static DataplexGCStoBQ of(String... args) {
    CommandLine cmd = parseArguments(args);
    String customSqlGCSPath = cmd.getOptionValue(CUSTOM_SQL_GCS_PATH_OPTION);
    String asset = cmd.getOptionValue(ASSET_LIST_OPTION);
    String entitiesString = cmd.getOptionValue(ENTITY_LIST_OPTION);
    String partitionField = cmd.getOptionValue(PARTITION_FIELD_OPTION);
    String partitionType = cmd.getOptionValue(PARTITION_TYPE_OPTION);
    return new DataplexGCStoBQ(
        customSqlGCSPath, entitiesString, asset, partitionField, partitionType);
  }

  /**
   * Sets value for entityList based on input values for entitiesString and asset
   *
   * @throws Exception if values are passed for both --dataplexEntityList and --dataplexAsset. Will
   *     also throw exceptio if niether is set
   */
  private void checkInput() throws Exception {
    if (entitiesString != null && asset != null) {
      throw new Exception(
          String.format(
              "Properties %s and %s ars mutually exclusive, please specify just one of these.",
              ENTITY_LIST_OPTION, ASSET_LIST_OPTION));
    } else if (asset != null) {
      this.entityList = DataplexUtil.getEntityNameListFromAsset(asset);
    } else if (entitiesString != null) {
      this.entityList = Arrays.asList(entitiesString.split(","));
    } else {
      throw new Exception(
          String.format("Please specifiy either %s or %s", ENTITY_LIST_OPTION, ASSET_LIST_OPTION));
    }
  }

  /**
   * Parse command line arguments
   *
   * @param args line arguments
   * @return parsed arguments
   */
  public static CommandLine parseArguments(String... args) {
    Options options = new Options();
    Option customSQLFileOption =
        new Option(CUSTOM_SQL_GCS_PATH_OPTION, "GCS path of file containing custom sql");
    customSQLFileOption.setRequired(false);
    customSQLFileOption.setArgs(1);
    options.addOption(customSQLFileOption);

    Option entityListOption = new Option(ENTITY_LIST_OPTION, "Dataplex GCS table resource name");
    entityListOption.setRequired(false);
    entityListOption.setArgs(2);
    options.addOption(entityListOption);

    Option assetOption = new Option(ASSET_LIST_OPTION, "Dataplex asset name");
    assetOption.setRequired(false);
    assetOption.setArgs(3);
    options.addOption(assetOption);

    Option partitionField = new Option(PARTITION_FIELD_OPTION, "BigQuery partitionField");
    partitionField.setRequired(false);
    partitionField.setArgs(4);
    options.addOption(partitionField);

    Option partitionType = new Option(PARTITION_TYPE_OPTION, "BigQuery partitionType");
    partitionType.setRequired(false);
    partitionType.setArgs(5);
    options.addOption(partitionType);

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
    LOGGER.info("Reading target table: {}", targetTable);
    spark.conf().set("viewsEnabled", "true");
    spark.conf().set("materializationDataset", targetDataset);
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
    if (bqPartitionsKeysDS == null
        || incrementalParittionCopy.equals(INCREMENTAL_PARTITION_COPY_NO)) {
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
      String path = row.get(0).toString() + "/*";
      LOGGER.info("Loading data from GCS path: {}", path);
      Dataset<Row> newPartitionTempDS =
          sqlContext
              .read()
              .format(inputFileFormat)
              .option(GCS_BQ_CSV_HEADER, true)
              .option(GCS_BQ_CSV_INFOR_SCHEMA, true)
              .option(DATAPLEX_GCS_BQ_BASE_PATH_PROP_NAME, entityBasePath)
              .load(path);
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
      LOGGER.info("Reading custom SQL from GCS path: {}", customSqlGCSPath);
      String[] pathAsList = customSqlGCSPath.replace("gs://", "").split("/");
      String BUCKET_NAME = pathAsList[0];
      String OBJECT_NAME = Stream.of(pathAsList).skip(1).collect(Collectors.joining("/"));

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
      LOGGER.info("Writing to target table: {}", targetTable);
      DataFrameWriter dfWriter =
          newPartitionsDf
              .write()
              .format(GCS_BQ_OUTPUT_FORMAT)
              .option(GCS_BQ_OUTPUT, targetTable)
              .option(GCS_BQ_TEMP_BUCKET, bqTempBucket)
              .option(
                  DATAPLEX_GCS_BQ_CREATE_DISPOSITION_PROP_NAME,
                  DATAPLEX_GCS_BQ_CREATE_DISPOSITION_CREATE_IF_NEEDED)
              .mode(sparkSaveMode);

      if (partitionField != null) {
        dfWriter.option(DATAPLEX_GCS_BQ_PARTITION_FIELD_PROP_NAME, partitionField);
      }
      if (partitionType != null) {
        dfWriter.option(DATAPLEX_GCS_BQ_PARTITION_TYPE_PROP_NAME, partitionType);
      }

      dfWriter.save();
    }
  }

  public void runTemplate() {
    try {
      this.spark = SparkSession.builder().appName("Dataplex GCS to BQ").getOrCreate();
      this.sqlContext = new SQLContext(spark);
      checkInput();

      for (int i = 0; i < entityList.size(); i += 1) {
        this.entity = entityList.get(i);
        this.entityBasePath = DataplexUtil.getBasePathEntityData(entity);
        this.inputFileFormat = DataplexUtil.getInputFileFormat(entity);
        this.targetTableName = entity.split("/")[entity.split("/").length - 1];
        this.targetTable = String.format("%s.%s.%s", projectId, targetDataset, targetTableName);
        LOGGER.info("Processing entity: {}", entity);

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
            getNewPartitionsPathsDS(
                partitionKeysList, dataplexPartitionsKeysDS, bqPartitionsKeysDS);

        // load data from each partition
        Dataset<Row> newPartitionsDS = getNewPartitionsDS(newPartitionsPathsDS);

        newPartitionsDS = DataplexUtil.castDatasetToDataplexSchema(newPartitionsDS, entity);

        newPartitionsDS = applyCustomSql(newPartitionsDS);

        writeToBQ(newPartitionsDS);
      }

    } catch (Throwable th) {
      LOGGER.error("Exception in DataplexGCStoBQ", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
