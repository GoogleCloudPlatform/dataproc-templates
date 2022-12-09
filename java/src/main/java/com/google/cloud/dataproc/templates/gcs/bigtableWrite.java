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
package com.google.cloud.dataproc.templates.gcs;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.util.Iterator;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class bigtableWrite implements ForeachPartitionFunction<Row>, java.io.Serializable {

  public static final Logger LOGGER = LoggerFactory.getLogger(bigtableWrite.class);

  public void call(Iterator<Row> t) throws Exception {
    LOGGER.info("Inside Call");
    long timestamp1 = System.currentTimeMillis() * 1000;
    BigtableDataClient dataClient =
        BigtableDataClient.create("yadavaja-sandbox", "bt-templates-test");
    while (t.hasNext()) {

      Row row = t.next();

      LOGGER.info("The message is %s ", row.getString(0));
      String rowkey = "aaaaa";
      String columnfamily = "cf";
      String columnname = row.getString(0);
      String columnvalue = row.getString(1);
      try {
        RowMutation rowMutation =
            RowMutation.create("bus-data", rowkey)
                .setCell(columnfamily, columnname, timestamp1, columnvalue);

        dataClient.mutateRow(rowMutation);
        LOGGER.info("Successfully wrote row %s", rowkey);
      } catch (Exception e) {
        LOGGER.info("Error during WriteSimple: \n" + e.toString());
      }
    }
    dataClient.close();
  }
}
