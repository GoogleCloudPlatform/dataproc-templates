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
import sys
import re
from pyspark.sql import SparkSession, DataFrameWriter, DataFrame
from pyspark.sql.functions import regexp_replace, col, regexp_extract, explode, split, trim

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['HiveToGCSTemplate']


class HiveToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Hive to GCS
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.HIVE_GCS_INPUT_DATABASE}',
            dest=constants.HIVE_GCS_INPUT_DATABASE,
            required=True,
            help='Hive database for exporting data to GCS'
        )

        parser.add_argument(
            f'--{constants.HIVE_GCS_INPUT_TABLE}',
            dest=constants.HIVE_GCS_INPUT_TABLE,
            required=True,
            help='Hive table for exporting data to GCS'
        )
        parser.add_argument(
            f'--{constants.HIVE_GCS_OUTPUT_LOCATION}',
            dest=constants.HIVE_GCS_OUTPUT_LOCATION,
            required=True,
            help='GCS location for output files'
        )
        parser.add_argument(
            f'--{constants.HIVE_GCS_OUTPUT_FORMAT}',
            dest=constants.HIVE_GCS_OUTPUT_FORMAT,
            required=False,
            default=constants.FORMAT_PRQT,
            help=(
                'Output file format ' 
                '(one of: avro,parquet,csv,json) '
                '(Defaults to parquet)'
            ),
            choices=[
                constants.FORMAT_AVRO,
                constants.FORMAT_PRQT,
                constants.FORMAT_CSV,
                constants.FORMAT_JSON
            ]
        )
        parser.add_argument(
            f'--{constants.HIVE_GCS_OUTPUT_MODE}',
            dest=constants.HIVE_GCS_OUTPUT_MODE,
            required=False,
            default=constants.OUTPUT_MODE_OVERWRITE,
            help=(
                'Output write mode '
                '(one of: append,overwrite,ignore,errorifexists) '
                '(Defaults to overwrite)'
            ),
            choices=[
                constants.OUTPUT_MODE_OVERWRITE,
                constants.OUTPUT_MODE_APPEND,
                constants.OUTPUT_MODE_IGNORE,
                constants.OUTPUT_MODE_ERRORIFEXISTS
            ]
        )
        parser.add_argument(
            f'--{constants.HIVE_GCS_TEMP_VIEW_NAME}',
            dest=constants.HIVE_GCS_TEMP_VIEW_NAME,
            required=False,
            default="",
            help='Temp view name for creating a spark sql view on source data. This name has to match with the table name that will be used in the SQL query'
        )
        parser.add_argument(
            f'--{constants.HIVE_GCS_SQL_QUERY}',
            dest=constants.HIVE_GCS_SQL_QUERY,
            required=False,
            default="",
            help='SQL query for data transformation. This must use the temp view name as the table to query from.'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        if getattr(known_args, constants.HIVE_GCS_SQL_QUERY) and not getattr(known_args, constants.HIVE_GCS_TEMP_VIEW_NAME):
            sys.exit('ArgumentParser Error: Temp view name cannot be null if you want to do data transformations with query')

        return vars(known_args)

    def get_hive_partitions(self, input_table_name: str, spark: SparkSession) -> list:
        
        hive_create_stmt_df = spark.sql(f'show create table {input_table_name}')
        partition_col_names = hive_create_stmt_df.withColumn('replaced',regexp_replace('createtab_stmt','\n','')) \
                .withColumn("extracted_partitions",regexp_extract(col("replaced"),"PARTITIONED BY \((.*?)\)",1)) \
                .withColumn('splitted_partitions',explode(split(col("extracted_partitions"),","))) \
                .withColumn('partition_cols',split(trim(col("splitted_partitions"))," ")) \
                .selectExpr('regexp_replace(partition_cols[0],"`","") as column_names').collect()
        partition_cols=[row.column_names for row in partition_col_names if row.column_names.strip()]
    
        return partition_cols if partition_cols else None


    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        hive_database: str = args[constants.HIVE_GCS_INPUT_DATABASE]
        hive_table: str = args[constants.HIVE_GCS_INPUT_TABLE]
        output_location: str = args[constants.HIVE_GCS_OUTPUT_LOCATION]
        output_format: str = args[constants.HIVE_GCS_OUTPUT_FORMAT]
        output_mode: str = args[constants.HIVE_GCS_OUTPUT_MODE]
        hive_temp_view: str = args[constants.HIVE_GCS_TEMP_VIEW_NAME]
        sql_query: str = args[constants.HIVE_GCS_SQL_QUERY]

        logger.info(
            "Starting Hive to GCS spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_table_name = hive_database + "." + hive_table
        input_data = spark.table(input_table_name)
      
        if sql_query:
            # Create temp view on source data
            input_data.createGlobalTempView(hive_temp_view)
            # Execute SQL
            output_data = spark.sql(sql_query)
        else:
            output_data = input_data

        #Get partition columns from Hive
        partition_cols = self.get_hive_partitions(input_table_name,spark)
        logger.info(
                "Method get_hive_partitions: received partition columns as: "
                f"{partition_cols} for hive table {input_table_name}"
                )

        # Write
        writer: DataFrameWriter = output_data.write.mode(output_mode)
        if not sql_query.strip() and partition_cols:
            writer = writer.partitionBy(partition_cols)

        if output_format == constants.FORMAT_PRQT:
            writer.parquet(output_location)
        elif output_format == constants.FORMAT_AVRO:
            writer \
                .format(constants.FORMAT_AVRO) \
                .save(output_location)
        elif output_format == constants.FORMAT_CSV:
            writer \
                .option(constants.HEADER, True) \
                .csv(output_location)
        elif output_format == constants.FORMAT_JSON:
            writer.json(output_location)
