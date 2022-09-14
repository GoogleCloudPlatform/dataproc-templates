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

from pyspark.sql import SparkSession, DataFrame, DataFrameWriter

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants


__all__ = ['GCSToGCSTemplate']

class GCSToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from GCS into GCS post SQL transformation
    """
    
    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.GCS_TO_GCS_INPUT_LOCATION}',
            dest=constants.GCS_TO_GCS_INPUT_LOCATION,
            required=True,
            help='GCS location of the input files'
        )
        parser.add_argument(
            f'--{constants.GCS_TO_GCS_INPUT_FORMAT}',
            dest=constants.GCS_TO_GCS_INPUT_FORMAT,
            required=True,
            help='GCS input file format (one of: avro,parquet,csv,json)',
            choices=[
                constants.FORMAT_AVRO,
                constants.FORMAT_PRQT,
                constants.FORMAT_CSV,
                constants.FORMAT_JSON
            ]
        )
        parser.add_argument(
            f'--{constants.GCS_TO_GCS_TEMP_VIEW_NAME}',
            dest=constants.GCS_TO_GCS_TEMP_VIEW_NAME,
            required=False,
            default="",
            help='Temp view name for creating a spark sql view on source data. This name has to match with the table name that will be used in the SQL query'
        )
        parser.add_argument(
            f'--{constants.GCS_TO_GCS_SQL_QUERY}',
            dest=constants.GCS_TO_GCS_SQL_QUERY,
            required=False,
            default="",
            help='SQL query for data transformation. This must use the temp view name as the table to query from.'
        )
        
        parser.add_argument(
            f'--{constants.GCS_TO_GCS_OUTPUT_PARTITION_COLUMN}',
            dest=constants.GCS_TO_GCS_OUTPUT_PARTITION_COLUMN,
            required=False,
            default="",
            help='Partition column name to partition the final output in destination bucket'
        )
        parser.add_argument(
            f'--{constants.GCS_TO_GCS_OUTPUT_FORMAT}',
            dest=constants.GCS_TO_GCS_OUTPUT_FORMAT,
            required=False,
            default=constants.FORMAT_PRQT,
            help=(
                'Output write format '
                '(one of: avro,parquet,csv,json)'
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
            f'--{constants.GCS_TO_GCS_OUTPUT_MODE}',
            dest=constants.GCS_TO_GCS_OUTPUT_MODE,
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
        
        parser.add_argument(
            f'--{constants.GCS_TO_GCS_OUTPUT_LOCATION}',
            dest=constants.GCS_TO_GCS_OUTPUT_LOCATION,
            required=True,
            help=(
                'destination GCS location'
            )
        )
        
        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)
        
        if getattr(known_args, constants.GCS_TO_GCS_SQL_QUERY) and not getattr(known_args, constants.GCS_TO_GCS_TEMP_VIEW_NAME):
            sys.exit('ArgumentParser Error: Temp view name cannot be null if you want to do data transformations with query')

        return vars(known_args)
    
    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)
        # Arguments
        gcs_input_location: str = args[constants.GCS_TO_GCS_INPUT_LOCATION]
        gcs_input_format: str = args[constants.GCS_TO_GCS_INPUT_FORMAT]
        gcs_temp_view: str = args[constants.GCS_TO_GCS_TEMP_VIEW_NAME]
        sql_query: str = args[constants.GCS_TO_GCS_SQL_QUERY]
        output_partition_column: str = args[constants.GCS_TO_GCS_OUTPUT_PARTITION_COLUMN]
        gcs_output_mode: str = args[constants.GCS_TO_GCS_OUTPUT_MODE]
        gcs_output_format: str = args[constants.GCS_TO_GCS_OUTPUT_FORMAT]
        gcs_output_location: str = args[constants.GCS_TO_GCS_OUTPUT_LOCATION]
        
        logger.info(
            "Starting GCS to GCS with tranformations spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )
        
        # Read
        input_data: DataFrame
        
        if gcs_input_format == constants.FORMAT_PRQT:
            input_data = spark.read \
                .parquet(gcs_input_location)
        elif gcs_input_format == constants.FORMAT_AVRO:
            input_data = spark.read \
                .format(constants.FORMAT_AVRO) \
                .load(gcs_input_location)
        elif gcs_input_format == constants.FORMAT_CSV:
            input_data = spark.read \
                .format(constants.FORMAT_CSV) \
                .option(constants.HEADER, True) \
                .option(constants.INFER_SCHEMA, True) \
                .load(gcs_input_location)
        elif gcs_input_format == constants.FORMAT_JSON:
            input_data = spark.read \
                .json(gcs_input_location)
        
        if sql_query:
            # Create temp view on source data
            input_data.createOrReplaceTempView(gcs_temp_view)
            # Execute SQL
            output_data = spark.sql(sql_query)
        else:
            output_data = input_data
        
        # Write
        if output_partition_column:
            writer: DataFrameWriter = output_data.write.mode(gcs_output_mode).partitionBy(output_partition_column)
        else:
            writer: DataFrameWriter = output_data.write.mode(gcs_output_mode)
            
        if gcs_output_format == constants.FORMAT_PRQT:
            writer \
                .parquet(gcs_output_location)
        elif gcs_output_format == constants.FORMAT_AVRO:
            writer \
                .format(constants.FORMAT_AVRO) \
                .save(gcs_output_location)
        elif gcs_output_format == constants.FORMAT_CSV:
            writer \
                .option(constants.HEADER, True) \
                .csv(gcs_output_location)
        elif gcs_output_format == constants.FORMAT_JSON:
            writer \
                .json(gcs_output_location)