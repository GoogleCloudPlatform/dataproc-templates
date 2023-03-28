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

from pyspark.sql import SparkSession, DataFrameWriter

from dataproc_templates import BaseTemplate
from dataproc_templates.util.argument_parsing import add_spark_options
from dataproc_templates.util.dataframe_writer_wrappers import persist_dataframe_to_cloud_storage
import dataproc_templates.util.template_constants as constants


__all__ = ['BigQueryToGCSTemplate']


class BigQueryToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from BigQuery to Cloud Storage
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.BQ_GCS_INPUT_TABLE}',
            dest=constants.BQ_GCS_INPUT_TABLE,
            required=True,
            help='BigQuery Input table name'
        )
        parser.add_argument(
            f'--{constants.BQ_GCS_OUTPUT_FORMAT}',
            dest=constants.BQ_GCS_OUTPUT_FORMAT,
            required=True,
            help='Output file format (one of: avro,parquet,csv,json)',
            choices=[
                constants.FORMAT_AVRO,
                constants.FORMAT_PRQT,
                constants.FORMAT_CSV,
                constants.FORMAT_JSON
            ]
        )
        parser.add_argument(
            f'--{constants.BQ_GCS_OUTPUT_LOCATION}',
            dest=constants.BQ_GCS_OUTPUT_LOCATION,
            required=True,
            help='Cloud Storage location for output files'
        )
        parser.add_argument(
            f'--{constants.BQ_GCS_OUTPUT_MODE}',
            dest=constants.BQ_GCS_OUTPUT_MODE,
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
        add_spark_options(parser, constants.get_csv_output_spark_options("bigquery.gcs.output."))

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_table: str = args[constants.BQ_GCS_INPUT_TABLE]
        output_mode: str = args[constants.BQ_GCS_OUTPUT_MODE]

        output_location: str = args[constants.BQ_GCS_OUTPUT_LOCATION]
        output_format: str = args[constants.BQ_GCS_OUTPUT_FORMAT]

        logger.info(
            "Starting Bigquery to Cloud Storage Spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data = spark.read \
            .format(constants.FORMAT_BIGQUERY) \
            .option(constants.TABLE, input_table) \
            .load()

        # Write
        writer: DataFrameWriter = input_data.write.mode(output_mode)
        persist_dataframe_to_cloud_storage(writer, args, output_location, output_format, "bigquery.gcs.output.")
