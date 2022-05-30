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
import com.google.cloud.dataproc.templates.util.DataprocTemplateException;
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

  /**
   * This template will incrementally move data from a Dataplex GCS tables to BigQuery. It will
   * identify new partitions in Dataplex GCS and load them to BigQuery.
   */
  public static final Logger LOGGER = LoggerFactory.getLogger(GCStoBigquery.class);

  public static String CUSTOM_SQL_GCS_PATH_OPTION = "customSqlGcsPath";
  public static String ENTITY_OPTION = "dataplexEntity";
  public static String PARTITION_FIELD_OPTION = "partitionField";
  public static String PARTITION_TYPE_OPTION = "partitionType";
  public static String INCREMENTAL_PARTITION_COPY_NO = "no";
  public static String BQ_TABLE_NAME_FORMAT = "%s.%s.%s";
  public static String SPARK_SQL_SELECT_STAR = "*";
  public static String SPARK_SQL_SPLIT_VALUE_AND_GET_LOCATION =
      "split(value, ',')[0] as __gcs_location_path__";
  public static String SPARK_SQL_SPLIT_VALUE_AND_GET_KEY = "split(value, ',')[%d] as %s";
  public static String SPARK_SQL_VALUE_FIELD = "value";
  public static String SPARK_SQL_SELECT_DISTINCT_FROM = "select distinct %s FROM `%s`";
  public static String COMMA_DELIMITER = ",";
  public static String TABLE_NOT_FOUND_ERROR_CAUSE = "Not found: Table";
  public static String SPARK_SQL_GCS_LOCATION_PATH_COL_NAME = "__gcs_location_path__";
  public static String SPARK_SQL_DATAPLEX_PARTITION_KEYS_TEMP_VIEW_NAME =
      "dataplexPartitionsKeysDS";
  public static String SPARK_SQL_BQ_PARTITION_KEYS_TEMP_VIEW = "bqPartitionsKeysDS";
  public static String SPARK_SQL_COMPARE_COLS = "t1.%s=t2.%s";
  public static String SPARK_SQL_AND = " AND ";
  public static String SPARK_SQL_GET_NEW_PATHS =
      "SELECT %s FROM %s t1 LEFT JOIN %s t2 ON %s WHERE t2.id is null";
  public static String SPARK_SQL_OUTPUT_TABLE_TEMP_VIEW_NAME = "__table__";
  public static String GCS_PATH_PREFIX = "gs://";
  public static String EMPTY_STRING = "";
  public static String FORWARD_SLASH = "/";

  private SparkSession spark;
  private SQLContext sqlContext;

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
      String customSqlGCSPath, String entity, String partitionField, String partitionType) {
    this.customSqlGCSPath = customSqlGCSPath;
    this.entity = entity;
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
    String entity = cmd.getOptionValue(ENTITY_OPTION);
    String partitionField = cmd.getOptionValue(PARTITION_FIELD_OPTION);
    String partitionType = cmd.getOptionValue(PARTITION_TYPE_OPTION);
    return new DataplexGCStoBQ(customSqlGCSPath, entity, partitionField, partitionType);
  }

  /**
   * Sets value for entity
   *
   * @throws Exception if values no value is passed for --dataplexEntity and --dataplexAsset.
   */
  private void checkInput() throws DataprocTemplateException, IOException {
    if (entity != null) {
      this.entity = entity;
    } else {
      throw new DataprocTemplateException(String.format("Please specify %s", ENTITY_OPTION));
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

    Option entityListOption = new Option(ENTITY_OPTION, "Dataplex GCS table resource name");
    entityListOption.setRequired(false);
    entityListOption.setArgs(2);
    options.addOption(entityListOption);

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
        allPartitionsDf.selectExpr(SPARK_SQL_SELECT_STAR, SPARK_SQL_SPLIT_VALUE_AND_GET_LOCATION);
    for (int i = 0; i < partitionKeysList.size(); i += 1) {
      allPartitionsDf =
          allPartitionsDf.selectExpr(
              SPARK_SQL_SELECT_STAR,
              String.format(SPARK_SQL_SPLIT_VALUE_AND_GET_KEY, i + 1, partitionKeysList.get(i)));
    }
    allPartitionsDf = allPartitionsDf.drop(SPARK_SQL_VALUE_FIELD);
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
    spark.conf().set(SPARK_CONF_NAME_VIEWS_ENABLED, "true");
    spark.conf().set(SPARK_CONF_NAME_MATERIALIZATION_PROJECT, projectId);
    spark.conf().set(SPARK_CONF_NAME_MATERIALIZATION_DATASET, targetDataset);

    try {
      String sql =
          String.format(
              SPARK_SQL_SELECT_DISTINCT_FROM,
              String.join(COMMA_DELIMITER, partitionKeysList),
              targetTable);
      return spark.read().format(SPARK_READ_FORMAT_BIGQUERY).load(sql);
    } catch (BigQueryConnectorException e) {
      if (e.getCause().toString().contains(TABLE_NOT_FOUND_ERROR_CAUSE)) {
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
      return dataplexPartitionsKeysDS.select(SPARK_SQL_GCS_LOCATION_PATH_COL_NAME);
    }

    dataplexPartitionsKeysDS.createOrReplaceTempView(
        SPARK_SQL_DATAPLEX_PARTITION_KEYS_TEMP_VIEW_NAME);
    bqPartitionsKeysDS.createOrReplaceTempView(SPARK_SQL_BQ_PARTITION_KEYS_TEMP_VIEW);
    String joinClause =
        partitionKeysList.stream()
            .map(str -> String.format(SPARK_SQL_COMPARE_COLS, str, str))
            .collect(Collectors.joining(SPARK_SQL_AND));
    Dataset<Row> newPartitionsDS =
        spark.sql(
            String.format(
                SPARK_SQL_GET_NEW_PATHS,
                SPARK_SQL_GCS_LOCATION_PATH_COL_NAME,
                SPARK_SQL_DATAPLEX_PARTITION_KEYS_TEMP_VIEW_NAME,
                SPARK_SQL_BQ_PARTITION_KEYS_TEMP_VIEW,
                joinClause));
    return newPartitionsDS;
  }

  /**
   * Loads from GCS all new partitions
   *
   * @param newPartitionsPathsDf a dataset with the GCS paths of new partitions
   * @return a dataset with the GCS paths of new partitions
   */
  private Dataset<Row> getNewPartitionsDS(Dataset<Row> newPartitionsPathsDf) {
    Row[] result =
        (Row[]) newPartitionsPathsDf.select(SPARK_SQL_GCS_LOCATION_PATH_COL_NAME).collect();
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
  private Dataset<Row> applyCustomSql(Dataset<Row> newPartitionsDf) throws IOException {
    if (customSqlGCSPath != null) {
      LOGGER.info("Reading custom SQL from GCS path: {}", customSqlGCSPath);
      String[] pathAsList =
          customSqlGCSPath.replace(GCS_PATH_PREFIX, EMPTY_STRING).split(FORWARD_SLASH);
      String BUCKET_NAME = pathAsList[0];
      String OBJECT_NAME = Stream.of(pathAsList).skip(1).collect(Collectors.joining(FORWARD_SLASH));

      StorageOptions options =
          StorageOptions.newBuilder()
              .setProjectId(projectId)
              .setCredentials(GoogleCredentials.getApplicationDefault())
              .build();

      Storage storage = options.getService();
      Blob blob = storage.get(BUCKET_NAME, OBJECT_NAME);
      String custom_sql = new String(blob.getContent());
      newPartitionsDf.createOrReplaceTempView(SPARK_SQL_OUTPUT_TABLE_TEMP_VIEW_NAME);
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
  private void writeToBQ(Dataset<Row> newPartitionsDf) {
    if (newPartitionsDf != null) {
      LOGGER.info("Writing to target table: {}", targetTable);
      DataFrameWriter dfWriter =
          newPartitionsDf
              .write()
              .format(GCS_BQ_OUTPUT_FORMAT)
              .option(GCS_BQ_OUTPUT, targetTable)
              .option(GCS_BQ_TEMP_BUCKET, bqTempBucket)
              .option(INTERMEDIATE_FORMAT_OPTION_NAME, INTERMEDIATE_FORMAT_ORC)
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

      this.entityBasePath = DataplexUtil.getBasePathEntityData(entity);
      this.inputFileFormat = DataplexUtil.getInputFileFormat(entity);
      this.targetTableName = entity.split(FORWARD_SLASH)[entity.split(FORWARD_SLASH).length - 1];
      this.targetTable =
          String.format(BQ_TABLE_NAME_FORMAT, projectId, targetDataset, targetTableName);
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
          getNewPartitionsPathsDS(partitionKeysList, dataplexPartitionsKeysDS, bqPartitionsKeysDS);

      // load data from each partition
      Dataset<Row> newPartitionsDS = getNewPartitionsDS(newPartitionsPathsDS);
      newPartitionsDS.printSchema();

      newPartitionsDS = DataplexUtil.castDatasetToDataplexSchema(newPartitionsDS, entity);

      newPartitionsDS = applyCustomSql(newPartitionsDS);

      writeToBQ(newPartitionsDS);

    } catch (Throwable th) {
      LOGGER.error("Exception in DataplexGCStoBQ", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
