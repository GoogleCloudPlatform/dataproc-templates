#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import argparse
import pprint
import sys
import io
from logging import Logger
from typing import Any, Dict, Optional, Sequence

import dataproc_templates.util.template_constants as constants
import pandas as pd
from dataproc_templates import BaseTemplate
from google.cloud import storage
import pyspark
from pyspark.sql import DataFrameWriter, SparkSession
from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.bigquery.dataset import Dataset
from datetime import datetime


def WriteToCloud (ddls_to_rdd, bucket, path):
    # Write
    now = datetime.now()
    timedate = now.strftime("%m-%d-%Y %H.%M.%S")
    ddls_to_rdd.coalesce(1).saveAsTextFile("gs://"+bucket+"/"+path+timedate)

class HiveDDLExtractorTemplate(BaseTemplate): 
    """
    Dataproc template implementing exports from Hive to BigQuery
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.HIVE_DDL_EXTRACTOR_INPUT_DATABASE}',
            dest=constants.HIVE_DDL_EXTRACTOR_INPUT_DATABASE,
            required=True,
            help='Hive database for importing data to BigQuery'
        )

        parser.add_argument(
            f'--{constants.HIVE_DDL_EXTRACTOR_OUTPUT_BUCKET}',
            dest=constants.HIVE_DDL_EXTRACTOR_OUTPUT_BUCKET,
            required=True,
            help='Bucket for the output'
        )

        parser.add_argument(
            f'--{constants.HIVE_DDL_EXTRACTOR_OUTPUT_DATASET}',
            dest=constants.HIVE_DDL_EXTRACTOR_OUTPUT_DATASET,
            required=True,
            help='BigQuery output dataset name'
        )
        parser.add_argument(
            f'--{constants.HIVE_DDL_EXTRACTOR_OUTPUT_TABLE}',
            dest=constants.HIVE_DDL_EXTRACTOR_OUTPUT_TABLE,
            required=True,
            help='BigQuery output table name'
        )
       
        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)


    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:
        """
        
        Dataproc template allowing the extraction of DDLs from Hive Metastore

        """
        logger: Logger = self.get_logger(spark=spark)

        hive_database: str = args[constants.HIVE_DDL_EXTRACTOR_INPUT_DATABASE]
        gcs_working_directory: str = args[constants.HIVE_DDL_EXTRACTOR_OUTPUT_BUCKET]
        bigquery_dataset: str = args[constants.HIVE_DDL_EXTRACTOR_OUTPUT_DATASET]
        bigquery_table: str = args[constants.HIVE_DDL_EXTRACTOR_OUTPUT_TABLE]
        gcs_target_path = constants.HIVE_DDL_EXTRACTOR_OUTPUT_PATH
        gcs_staging_path="gs://"+gcs_working_directory+"/RawZone/"

        logger.info(
                "Starting Hive DDL Extraction job with parameters:\n"
                f"{pprint.pformat(args)}"
            )

        # databaseExists() checks if there is a database in the hive cluster that matches the system argument
        dbCheck = spark.catalog._jcatalog.databaseExists(hive_database)
        # dbCheck serves as a boolean. if true then the script will continue
        ddls = ""
        if dbCheck:
            tables = spark.catalog.listTables(hive_database)
            metadata=[]
            columns=['database','table','partition_string','format','hdfs_path','gcs_raw_zone_path']
            for t in tables:
                """ 
                The for loop iterates through all the tables within the database 
                """
                print("Extracting DDL for the Hive Table: "+hive_database+"."+t.name)
                show_create = spark.sql("SHOW CREATE TABLE {}.{}".format(hive_database, t.name))
                ddl_table = spark.sql(
                    "DESCRIBE FORMATTED {}.{}".format(hive_database, t.name))
                show_extended=spark.sql("show table extended from `"+hive_database+"` like '"+t.name+"'")
                ddl_table.registerTempTable("metadata")
                show_extended.registerTempTable("show_extended")
                info=spark.sql("select information from show_extended")
                partition_check=show_create.filter(show_create["createtab_stmt"].contains("PARTITIONED")).head(1)
                partition_string=""
                if partition_check:
                    partition=show_create.first()[0].split("PARTITIONED BY (")[1].split(")")[0]
                    partitions=partition.split(",")
                    for part in partitions:
                        partition_type=spark.sql("select data_type from metadata where col_name="+"'"+part.strip()+"'").first()[0]
                        partition_string=partition_string+" "+part+" "+partition_type+","
                    partition_string=partition_string[:-1]
                    partition_by="PARTITIONED BY ("+partition_string+")\n"
                    first_part=show_create.first()[0].split(partitions[0])[0][:-5].split(t.name+"`")[1:]
                    first_part="".join(first_part)+")\n"
                else:
                    partition_by=")\n"
                    first_part=show_create.first()[0].split(t.name)[1:]
                    first_part=show_create.first()[0].split(')')[0].split(t.name+"`")[1:]
                    first_part="".join(first_part)+")\n"
                db_name=spark.sql("select data_type from metadata where col_name='Database'").first()[0]
                table_name=spark.sql("select data_type from metadata where col_name='Table'").first()[0]
                hdfs_file_path=info.first()[0].split("Location: ")[1].split("\n")[0]
                format=show_create.first()[0].split("USING ")[1].split("\n")[0]
                
                #partition and format
                initial="CREATE TABLE IF NOT EXISTS "+t.name
                storage_format=format
                location_path=gcs_staging_path+hdfs_file_path.split('hdfs://')[1]
                metadata.append([db_name,table_name,partition_string,storage_format,hdfs_file_path,location_path])
                ddl = initial+first_part+partition_by+";\n"
                ddls = ddls + ddl
            rdd=spark.sparkContext.parallelize(metadata)
            metadata_df=rdd.toDF(columns)
            # Write metadata to BQ
            metadata_df.write \
                .format(constants.FORMAT_BIGQUERY) \
                .option(constants.TABLE, bigquery_dataset+'.'+bigquery_table) \
                .option(constants.TEMP_GCS_BUCKET, gcs_working_directory) \
                .mode(constants.OUTPUT_MODE_APPEND) \
                .save()
        else:
            logger.error("This database does not exist")
            exit (1)

        # Write the DDLs to GCS
        ddls_to_rdd = spark.sparkContext.parallelize([ddls])
        WriteToCloud(ddls_to_rdd, gcs_working_directory, gcs_target_path)