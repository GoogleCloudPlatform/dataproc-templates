# Copyright 2023 Google LLC
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
from dataproc_templates.util.argument_parsing import add_spark_options
from dataproc_templates.util.dataframe_reader_wrappers import ingest_dataframe_from_cloud_storage
from dataproc_templates.util.dataframe_writer_wrappers import persist_dataframe_to_cloud_storage


__all__ = ['GCSToGCSTemplate']


class GCSToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from Cloud Storage into Cloud Storage post SQL transformation
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:

        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.GCS_TO_GCS_INPUT_LOCATION}',
            dest=constants.GCS_TO_GCS_INPUT_LOCATION,
            required=True,
            help='Cloud Storage location of the input files'
        )
        parser.add_argument(
            f'--{constants.GCS_TO_GCS_INPUT_FORMAT}',
            dest=constants.GCS_TO_GCS_INPUT_FORMAT,
            required=True,
            help='Cloud Storage input file format (one of: avro,parquet,csv,json,delta)',
            choices=[
                constants.FORMAT_AVRO,
                constants.FORMAT_PRQT,
                constants.FORMAT_CSV,
                constants.FORMAT_JSON,
                constants.FORMAT_DELTA
            ]
        )
        add_spark_options(parser, constants.get_csv_input_spark_options("gcs.gcs.input."))
        add_spark_options(parser, constants.get_csv_output_spark_options("gcs.gcs.output."))
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
                'Destination Cloud Storage location'
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
        input_location: str = args[constants.GCS_TO_GCS_INPUT_LOCATION]
        input_format: str = args[constants.GCS_TO_GCS_INPUT_FORMAT]
        gcs_temp_view: str = args[constants.GCS_TO_GCS_TEMP_VIEW_NAME]
        sql_query: str = args[constants.GCS_TO_GCS_SQL_QUERY]
        output_partition_column: str = args[constants.GCS_TO_GCS_OUTPUT_PARTITION_COLUMN]
        output_mode: str = args[constants.GCS_TO_GCS_OUTPUT_MODE]
        output_format: str = args[constants.GCS_TO_GCS_OUTPUT_FORMAT]
        output_location: str = args[constants.GCS_TO_GCS_OUTPUT_LOCATION]

        logger.info(
            "Starting Cloud Storage to Cloud Storage with tranformations Spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data = ingest_dataframe_from_cloud_storage(
            spark,
            args,
            input_location,
            input_format,
            "gcs.gcs.input.",
            avro_format_override=constants.FORMAT_AVRO
        )

        if sql_query:
            # Create temp view on source data
            input_data.createOrReplaceTempView(gcs_temp_view)
            # Execute SQL
            output_data = spark.sql(sql_query)
        else:
            output_data = input_data

        # Write
        if output_partition_column:
            writer: DataFrameWriter = output_data.write.mode(output_mode).partitionBy(output_partition_column)
        else:
            writer: DataFrameWriter = output_data.write.mode(output_mode)

        persist_dataframe_to_cloud_storage(writer, args, output_location, output_format, "gcs.gcs.output.")