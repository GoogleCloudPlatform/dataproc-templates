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
import com.google.cloud.dataproc.templates.util.Dataplex.DataplexAssetUtil;
import com.google.cloud.dataproc.templates.util.Dataplex.DataplexEntityUtil;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DataFrameReader;
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
  public static String TARGET_TABLE_NAME_OPTION = "targetTableName";
  public static String INCREMENTAL_PARTITION_COPY_NO = "no";
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
      "SELECT %s FROM %s t1 LEFT JOIN %s t2 ON %s WHERE t2.%s is null";
  public static String SPARK_SQL_OUTPUT_TABLE_TEMP_VIEW_NAME = "__table__";
  public static String GCS_PATH_PREFIX = "gs://";
  public static String EMPTY_STRING = "";
  public static String FORWARD_SLASH = "/";

  private SparkSession spark;
  private SQLContext sqlContext;

  private DataplexEntityUtil sourceEntityUtil;

  private Dataset<Row> newDataDS;
  private String sourceEntity;
  private String partitionField;
  private String partitionType;

  private String projectId;
  private String customSqlGCSPath;
  private String sourceEntityBasePath;
  private String targetDataset;
  private String targetTableName;
  private String targetTable;
  private String targetAsset;
  private String targetEntity;
  private String inputFileFormat;
  private String inputCSVDelimiter;
  private String bqTempBucket;
  private String sparkSaveMode;
  private String incrementalParittionCopy;
  private String sparkLogLevel;

  public DataplexGCStoBQ(
      String customSqlGCSPath,
      String sourceEntity,
      String partitionField,
      String partitionType,
      String targetTableName) {
    this.customSqlGCSPath = customSqlGCSPath;
    this.sourceEntity = sourceEntity;
    this.partitionField = partitionField;
    this.partitionType = partitionType;
    this.targetTableName = targetTableName;

    projectId = getProperties().getProperty(PROJECT_ID_PROP);
    targetDataset = getProperties().getProperty(DATAPLEX_GCS_BQ_TARGET_DATASET);
    targetAsset = getProperties().getProperty(DATAPLEX_GCS_BQ_TARGET_ASSET);
    targetEntity = getProperties().getProperty(DATAPLEX_GCS_BQ_TARGET_ENTITY);
    bqTempBucket = getProperties().getProperty(GCS_BQ_LD_TEMP_BUCKET_NAME);
    sparkSaveMode = getProperties().getProperty(DATAPLEX_GCS_BQ_SAVE_MODE);
    sparkLogLevel = getProperties().getProperty(SPARK_LOG_LEVEL);
    incrementalParittionCopy =
        getProperties().getProperty(DATAPLEX_GCS_BQ_INCREMENTAL_PARTITION_COPY);
    if (incrementalParittionCopy.equals(INCREMENTAL_PARTITION_COPY_NO)) {
      sparkSaveMode = SaveMode.Overwrite.toString();
    }
  }

  public static DataplexGCStoBQ of(String... args) {
    CommandLine cmd = parseArguments(args);
    String customSqlGCSPath = cmd.getOptionValue(CUSTOM_SQL_GCS_PATH_OPTION);
    String sourceEntity = cmd.getOptionValue(ENTITY_OPTION);
    String partitionField = cmd.getOptionValue(PARTITION_FIELD_OPTION);
    String partitionType = cmd.getOptionValue(PARTITION_TYPE_OPTION);
    String targetTableName = cmd.getOptionValue("targetTableName");
    return new DataplexGCStoBQ(
        customSqlGCSPath, sourceEntity, partitionField, partitionType, targetTableName);
  }

  /**
   * Sets value for entity
   *
   * @throws Exception if values no value is passed for --dataplexEntity
   */
  public void validateInput() {
    if (sourceEntity != null) {
      this.sourceEntity = sourceEntity;
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
  public static CommandLine parseArguments(String[] args) {
    Options options = new Options();

    CommandLineParser parser = new DefaultParser();
    Option entityListOption =
        OptionBuilder.withLongOpt(ENTITY_OPTION)
            .hasArgs(1)
            .isRequired(true)
            .withDescription("Dataplex GCS table resource name")
            .create();

    Option customSQLFileOption =
        OptionBuilder.withLongOpt(CUSTOM_SQL_GCS_PATH_OPTION)
            .hasArgs(1)
            .isRequired(false)
            .withDescription("GCS path of file containing custom sql")
            .create();

    Option partitionField =
        OptionBuilder.withLongOpt(PARTITION_FIELD_OPTION)
            .hasArgs(1)
            .isRequired(false)
            .withDescription("BigQuery partitionField")
            .create();

    Option partitionType =
        OptionBuilder.withLongOpt(PARTITION_TYPE_OPTION)
            .hasArgs(1)
            .isRequired(false)
            .withDescription("BigQuery partitionType")
            .create();

    Option targetTableName =
        OptionBuilder.withLongOpt(TARGET_TABLE_NAME_OPTION)
            .hasArgs(1)
            .isRequired(false)
            .withDescription("BigQuery table name where data will be written to")
            .create();

    options =
        new Options()
            .addOption(entityListOption)
            .addOption(customSQLFileOption)
            .addOption(partitionField)
            .addOption(partitionType)
            .addOption(targetTableName);
    try {
      return parser.parse(options, args, true);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Execute request on Google API to fetch schema of a Dataplex entity and parses out a list of
   * partition Keys
   *
   * @return list with partition keys of the entity, return null if Dataplex entity has no
   *     partitions
   * @throws IOException when request on Dataplex API fails
   */
  private List<String> getPartitionKeyList() throws IOException {
    try {
      return this.sourceEntityUtil.getPartitionKeyList();
    } catch (DataplexEntityUtil.DataplexEntityUtilNoPartitionError e) {
      LOGGER.info("Source data has no partitions, performing a full load");
      return null;
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
                joinClause,
                partitionKeysList.get(0)));
    return newPartitionsDS;
  }

  /**
   * Creates DataFrameReader taking into account input file format and csv delimeter when applicable
   *
   * @return a DataFrameReader
   */
  private DataFrameReader getDataFrameReader() {
    DataFrameReader DfReader =
        sqlContext
            .read()
            .format(inputFileFormat)
            .option(DATAPLEX_GCS_BQ_BASE_PATH_PROP_NAME, sourceEntityBasePath);

    if (this.inputFileFormat.equals(GCS_BQ_CSV_FORMAT)) {
      DfReader.option(GCS_BQ_CSV_HEADER, true)
          .option(GCS_BQ_CSV_INFOR_SCHEMA, true)
          .option(GCS_BQ_CSV_DELIMITER_PROP_NAME, this.inputCSVDelimiter);
    }
    return DfReader;
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
    DataFrameReader DfReader = getDataFrameReader();
    for (Row row : result) {
      String path = row.get(0).toString() + "/*";
      LOGGER.info("Loading data from GCS path: {}", path);
      Dataset<Row> newPartitionTempDS = DfReader.load(path);

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

  public void checkTarget() throws IOException {
    if (!StringUtils.isAllBlank(this.targetEntity)) {
      DataplexEntityUtil dataplexTargetEntityUtil = new DataplexEntityUtil(this.targetEntity);
      this.targetTable = dataplexTargetEntityUtil.getTableFullName();
      this.targetDataset = this.targetTable.split("\\.")[1];
    } else {
      if (!StringUtils.isAllBlank(this.targetAsset)) {
        DataplexAssetUtil dataplexAssetUtil = new DataplexAssetUtil(this.targetAsset);
        this.projectId = dataplexAssetUtil.getProjectId();
        this.targetDataset = dataplexAssetUtil.getDatasetName();
      }
      if (this.targetTableName == null) {
        this.targetTableName =
            this.sourceEntity
                .split(FORWARD_SLASH)[this.sourceEntity.split(FORWARD_SLASH).length - 1];
      }
      this.targetTable =
          String.format(BQ_TABLE_NAME_FORMAT, projectId, targetDataset, targetTableName);
    }
  }

  public void runTemplate() {
    try {
      this.spark = SparkSession.builder().appName("Dataplex GCS to BQ").getOrCreate();

      // Set log level
      this.spark.sparkContext().setLogLevel(sparkLogLevel);

      this.sqlContext = new SQLContext(spark);
      validateInput();

      this.sourceEntityUtil = new DataplexEntityUtil(this.sourceEntity);
      this.sourceEntityBasePath = sourceEntityUtil.getBasePathEntityData();
      this.inputFileFormat = sourceEntityUtil.getInputFileFormat();

      // checking for CSV delimiter when applicable
      if (this.inputFileFormat.equals(GCS_BQ_CSV_FORMAT)) {
        this.inputCSVDelimiter = sourceEntityUtil.getInputCSVDelimiter();
      }

      // checking for user provided target table name
      checkTarget();

      // listing source data partitions
      List<String> partitionKeysList = getPartitionKeyList();

      // if source data has no partitions a full load and overwrite is performed
      if (partitionKeysList == null) {
        newDataDS = getDataFrameReader().load(this.sourceEntityBasePath);
        this.sparkSaveMode = SPARK_SAVE_MODE_OVERWRITE;
      } else {
        List<String> partitionsListWithLocationAndKeys =
            sourceEntityUtil.getPartitionsListWithLocationAndKeys();

        // Building dataset with all partitions keys in Dataplex Entity
        Dataset<Row> dataplexPartitionsKeysDS =
            getAllPartitionsDf(partitionsListWithLocationAndKeys, partitionKeysList);

        // Querying BQ for all partition keys currently present in target table
        Dataset<Row> bqPartitionsKeysDS = getBQTargetAvailablePartitionsDf(partitionKeysList);

        // Compare dataplexPartitionsKeysDS and bqPartitionsKeysDS to identify new
        // partitions
        Dataset<Row> newPartitionsPathsDS =
            getNewPartitionsPathsDS(
                partitionKeysList, dataplexPartitionsKeysDS, bqPartitionsKeysDS);

        // load data from each partition
        newDataDS = getNewPartitionsDS(newPartitionsPathsDS);
      }

      if (newDataDS != null) {
        newDataDS = this.sourceEntityUtil.castDatasetToDataplexSchema(newDataDS);
        newDataDS = applyCustomSql(newDataDS);
        writeToBQ(newDataDS);
      } else {
        LOGGER.info("No new partitions found");
      }

    } catch (Throwable th) {
      LOGGER.error("Exception in DataplexGCStoBQ", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
      throw new DataprocTemplateException(th.getMessage());
    }
  }
}
