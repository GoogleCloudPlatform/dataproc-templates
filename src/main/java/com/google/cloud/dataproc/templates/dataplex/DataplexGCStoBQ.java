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

import com.google.cloud.dataproc.templates.util.DataplexUtil;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class DataplexGCStoBQ {
  private SparkSession spark;
  private SQLContext sqlContext;

  /**
   * Parse partitionsListWithLocationAndKeys into a Dataset partition path and key values for each
   * partition in Dataplex Entity
   *
   * @param partitionsListWithLocationAndKeys list with partition path and key values for each
   *     partition in Dataplex Entity
   * @param partitionKeysList list of partition keys for Dataplex Entity
   * @return a dataset with partition path and key values for each partition in Dataplex Entity
   */
  public static Dataset<Row> getAllPartitionsDf(
      List<String> partitionsListWithLocationAndKeys,
      List<String> partitionKeysList,
      SQLContext sqlContext) {
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
  private static Dataset<Row> getBQTargetAvailablePartitionsDf(
      List<String> partitionKeysList, SparkSession spark) {
    spark.conf().set("viewsEnabled", "true");
    spark.conf().set("materializationDataset", "whaite_zone1");
    String sql =
        String.format(
            "select distinct %s FROM `yadavaja-sandbox.whaite_zone1.trips_1_test`",
            String.join(",", partitionKeysList));
    return spark.read().format("bigquery").load(sql);
  }

  /**
   * Compare partitions in dataplex (partitionValuesInDataplex) with data available at BQ target
   * table (bqTargetAvailablePartitionsDf) to identify new partitions
   *
   * @param partitionKeysList list of partition keys for Dataplex Entity
   * @return a dataset with the GCS paths of new partitions
   */
  private static Dataset<Row> getNewPartitionsPathDf(
      List<String> partitionKeysList, SparkSession spark) {
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
  private static Dataset<Row> getNewPartitionsDf(
      Dataset<Row> newPartitionsPathsDf, SQLContext sqlContext) {
    Row[] result = (Row[]) newPartitionsPathsDf.select("__gcs_location_path__").collect();
    Dataset newPartitionsDf = null;
    for (Row row : result) {
      Dataset newPartition =
          sqlContext
              .read()
              .format("csv")
              .option("header", true)
              .option("inferSchema", true)
              .option("basePath", "gs://whaite-dataplex-test-2/trips/")
              .load(row.get(0).toString() + "/*.csv");
      if (newPartitionsDf == null) {
        newPartitionsDf = newPartition;
      } else {
        newPartitionsDf = newPartitionsDf.union(newPartition);
      }
    }
    return newPartitionsDf;
  }

  public static void main(String... args) throws IOException {
    SparkSession spark =
        SparkSession.builder().master("local").appName("Dataplex GCS to BQ").getOrCreate();
    SQLContext sqlContext = new SQLContext(spark);

    String entity = args[0];

    List<String> partitionKeysList = DataplexUtil.getPartitionKeyList(entity);
    List<String> partitionsListWithLocationAndKeys =
        DataplexUtil.getPartitionsListWithLocationAndKeys(entity);

    // Building DF with all available partitions in Dataplex Entity
    Dataset<Row> allPartitionsDf =
        getAllPartitionsDf(partitionsListWithLocationAndKeys, partitionKeysList, sqlContext);
    allPartitionsDf.createOrReplaceTempView("partitionValuesInDataplex");

    // Querying BQ target table for all combinations of partition keys
    Dataset<Row> bqTargetAvailablePartitionsDf =
        getBQTargetAvailablePartitionsDf(partitionKeysList, spark);
    bqTargetAvailablePartitionsDf.createOrReplaceTempView("bqTargetAvailablePartitionsDf");

    // Compare partitionValuesInDataplex and bqTargetAvailablePartitionsDf to indetidy new
    // partitions
    Dataset<Row> newPartitionsPathsDf = getNewPartitionsPathDf(partitionKeysList, spark);

    // load data from each partition
    Dataset<Row> newPartitionsDf = getNewPartitionsDf(newPartitionsPathsDf, sqlContext);
    newPartitionsDf.show();

  }
}
