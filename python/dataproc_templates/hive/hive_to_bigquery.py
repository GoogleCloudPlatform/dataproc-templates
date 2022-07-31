# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, Sequence, Optional, Any
from logging import Logger
import argparse
import pprint
import pandas as pd
from pyspark.sql import SparkSession
import datetime
from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['HiveToBigQueryTemplate']


class HiveToBigQueryTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Hive to BigQuery
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.HIVE_BQ_INPUT_DATABASE}',
            dest=constants.HIVE_BQ_INPUT_DATABASE,
            required=True,
            help='Hive database for importing data to BigQuery'
        )

        parser.add_argument(
            f'--{constants.HIVE_BQ_INPUT_TABLE}',
            dest=constants.HIVE_BQ_INPUT_TABLE,
            required=True,
            help='Hive table for importing data to BigQuery'
        )
        parser.add_argument(
            f'--{constants.HIVE_DATABASE_ALL_TABLES}',
            dest=constants.HIVE_DATABASE_ALL_TABLES,
            required=True,
            help='All Hive tables present in HIVE Database'
        )

        parser.add_argument(
            f'--{constants.HIVE_BQ_OUTPUT_DATASET}',
            dest=constants.HIVE_BQ_OUTPUT_DATASET,
            required=True,
            help='BigQuery dataset for the output table'
        )

        parser.add_argument(
            f'--{constants.HIVE_BQ_LD_TEMP_BUCKET_NAME}',
            dest=constants.HIVE_BQ_LD_TEMP_BUCKET_NAME,
            required=True,
            help='Spark BigQuery connector temporary bucket'
        )

        parser.add_argument(
            f'--migration_id',
            dest="migration_id",
            default='00000000',
            help='Randomly generated id for every job'
        )


        parser.add_argument(
            f'--{constants.HIVE_BQ_OUTPUT_MODE}',
            dest=constants.HIVE_BQ_OUTPUT_MODE,
            required=False,
            default=constants.OUTPUT_MODE_APPEND,
            help=(
                'Output write mode '
                '(one of: append,overwrite,ignore,errorifexists) '
                '(Defaults to append)'
            ),
            choices=[
                constants.OUTPUT_MODE_OVERWRITE,
                constants.OUTPUT_MODE_APPEND,
                constants.OUTPUT_MODE_IGNORE,
                constants.OUTPUT_MODE_ERRORIFEXISTS
            ]
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)
        audit_df = pd.DataFrame(columns=["Migration_id","Source_DB_Name","Source_Table_Name","Target_DB_Name","Target_Table_Name","Job_Start_Time","Job_End_Time","Job_Status"])
        audit_dict={}

        # Arguments
        hive_database: str = args[constants.HIVE_BQ_INPUT_DATABASE]
        hive_table: str = args[constants.HIVE_BQ_INPUT_TABLE]
        bigquery_dataset: str = args[constants.HIVE_BQ_OUTPUT_DATASET]
        bq_temp_bucket: str = args[constants.HIVE_BQ_LD_TEMP_BUCKET_NAME]
        output_mode: str = args[constants.HIVE_BQ_OUTPUT_MODE]
        hive_database_all_tables: str = args[constants.HIVE_DATABASE_ALL_TABLES]
        migration_id: str = args["migration_id"]

        logger.info(
            "Starting Hive to Bigquery spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        for tbl in hive_database_all_tables.split(","):

            if hive_table=="*" or tbl.lower() in hive_table.lower().split(","):

                print("Loading Table: "+tbl)
                audit_dict["Migration_id"]=migration_id
                audit_dict["Source_DB_Name"]=hive_database
                audit_dict["Source_Table_Name"]=tbl
                audit_dict["Target_DB_Name"]=bigquery_dataset
                audit_dict["Target_Table_Name"]=tbl
                audit_dict["Job_Start_Time"]=str(datetime.datetime.now())

                try:
                    print("Loading Table :"+tbl)
                    # Read
                    input_data = spark.table(hive_database + "." + tbl)
                    # Write
                    input_data.write \
                        .format(constants.FORMAT_BIGQUERY) \
                        .option(constants.TABLE, bigquery_dataset + "." + tbl) \
                        .option(constants.TEMP_GCS_BUCKET, bq_temp_bucket) \
                        .mode(output_mode) \
                        .save()
                except Exception as e:
                    print(str(e))
                    audit_dict["Job_Status"]="Fail"
                    audit_dict["Job_End_Time"]=str(datetime.datetime.now())
                else:
                    print("Table {} loaded".format(tbl))
                    audit_dict["Job_Status"]="Pass"
                    audit_dict["Job_End_Time"]=str(datetime.datetime.now())
                audit_df=audit_df.append(audit_dict, ignore_index = True)

            else:
                print("{} Table not present in Hive Database {}".format(tbl,hive_database))

        #save audit df to GCS
        if audit_df.empty:
            print("Audit Dataframe is Empty")
        else:
            print(audit_df)
            newdf=spark.createDataFrame(audit_df)
            newdf.show()
            print("Moving Audit Dataframe to GCS Path {}".format("gs://"+bq_temp_bucket+"/audit/"+migration_id+"/"))
            newdf.coalesce(1).write.mode("append").csv("gs://"+bq_temp_bucket+"/audit/migrationId-"+migration_id+"/")
