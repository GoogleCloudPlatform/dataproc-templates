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

import com.google.cloud.dataproc.templates.BaseTemplate;
import com.google.cloud.dataproc.templates.gcs.GCStoBigquery;
import com.google.cloud.dataproc.templates.util.DataplexUtil;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
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

  private SparkSession spark;
  private SQLContext sqlContext;
  private String entityDataBasePath;
  private String entity;
  private String materializationDataset;
  private String targetTable;
  private String inputFileFormat;
  private String bqTempBucket;

  public DataplexGCStoBQ() {
    entity = getProperties().getProperty(DATAPLEX_GCS_BQ_ENTITY);
    materializationDataset = getProperties().getProperty(DATAPLEX_GCS_BQ_MATERIALIZATION_DATASET);
    targetTable = getProperties().getProperty(DATAPLEX_GCS_BQ_TARGET_TABLE);
    bqTempBucket = getProperties().getProperty(GCS_BQ_LD_TEMP_BUCKET_NAME);
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
  private Dataset<Row> getNewPartitionsPathDf(
      List<String> partitionKeysList,
      Dataset<Row> allPartitionsDf,
      Dataset<Row> bqTargetAvailablePartitionsDf) {
    if (bqTargetAvailablePartitionsDf == null) {
      return allPartitionsDf.select("__gcs_location_path__");
    }

    allPartitionsDf.createOrReplaceTempView("partitionValuesInDataplex");
    bqTargetAvailablePartitionsDf.createOrReplaceTempView("bqTargetAvailablePartitionsDf");
    String joinClause =
        partitionKeysList.stream()
            .map(str -> String.format("t1.%s=t2.%s", str, str))
            .collect(Collectors.joining(" AND "));
    Dataset newPartitionsDf =
        spark.sql(
            "SELECT __gcs_location_path__ "
                + "FROM partitionValuesInDataplex t1 "
                + "LEFT JOIN bqTargetAvailablePartitionsDf t2 ON "
                + joinClause
                + " WHERE t2.id is null");
    return newPartitionsDf;
  }

  /**
   * Read from GCS all new partitions
   *
   * @param newPartitionsPathsDf a dataset with the GCS paths of new partitions
   * @return a dataset with the GCS paths of new partitions
   */
  private Dataset<Row> getNewPartitionsDf(Dataset<Row> newPartitionsPathsDf) {
    Row[] result = (Row[]) newPartitionsPathsDf.select("__gcs_location_path__").collect();
    Dataset<Row> newPartitionsDf = null;
    for (Row row : result) {
      Dataset<Row> newPartitionTempDf =
          sqlContext
              .read()
              .format(inputFileFormat)
              .option(GCS_BQ_CSV_HEADER, true)
              .option(GCS_BQ_CSV_INFOR_SCHEMA, true)
              .option(DATAPLEX_GCS_BQ_BASE_PATH_PROP_NAME, entityDataBasePath)
              .load(row.get(0).toString() + "/*");
      if (newPartitionsDf == null) {
        newPartitionsDf = newPartitionTempDf;
      } else {
        newPartitionsDf = newPartitionsDf.union(newPartitionTempDf);
      }
    }
    return newPartitionsDf;
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

      // Building DF with all available partitions in Dataplex Entity
      Dataset<Row> allPartitionsDf =
          getAllPartitionsDf(partitionsListWithLocationAndKeys, partitionKeysList);

      // Querying BQ target table for all combinations of partition keys
      Dataset<Row> bqTargetAvailablePartitionsDf =
          getBQTargetAvailablePartitionsDf(partitionKeysList);

      // Compare partitionValuesInDataplex and bqTargetAvailablePartitionsDf to indetidy new
      // partitions
      Dataset<Row> newPartitionsPathsDf =
          getNewPartitionsPathDf(partitionKeysList, allPartitionsDf, bqTargetAvailablePartitionsDf);

      // load data from each partition
      Dataset<Row> newPartitionsDf = getNewPartitionsDf(newPartitionsPathsDf);

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

    } catch (Throwable th) {
      LOGGER.error("Exception in DataplexGCStoBQ", th);
      if (Objects.nonNull(spark)) {
        spark.stop();
      }
    }
  }
}
