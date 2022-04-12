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
import argparse
import pprint

from pyspark.sql import SparkSession

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['BigQueryToGCSTemplate']


class BigQueryToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from BigQuery to GCS
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
                constants.BQ_GCS_OUTPUT_FORMAT_AVRO,
                constants.BQ_GCS_OUTPUT_FORMAT_PARQUET,
                constants.BQ_GCS_OUTPUT_FORMAT_CSV,
                constants.BQ_GCS_OUTPUT_FORMAT_JSON
            ]
        )
        parser.add_argument(
            f'--{constants.BQ_GCS_OUTPUT_MODE}',
            dest=constants.BQ_GCS_OUTPUT_MODE,
            required=True,
            help='Output write mode (one of: append,overwrite)',
            choices=[
                constants.BQ_GCS_OUTPUT_MODE_OVERWRITE,
                constants.BQ_GCS_OUTPUT_MODE_APPEND
            ]
        )
        parser.add_argument(
            f'--{constants.BQ_GCS_OUTPUT_LOCATION}',
            dest=constants.BQ_GCS_OUTPUT_LOCATION,
            required=True,
            help='GCS location for output files'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        log4jLogger = spark.sparkContext._jvm.org.apache.log4j  # pylint: disable=protected-access,invalid-name
        logger = log4jLogger.LogManager.getLogger(__name__)

        # Arguments
        input_table = args[constants.BQ_GCS_INPUT_TABLE]
        output_format = args[constants.BQ_GCS_OUTPUT_FORMAT]
        output_mode = args[constants.BQ_GCS_OUTPUT_MODE]
        output_location = args[constants.BQ_GCS_OUTPUT_LOCATION]

        logger.info(
            "Starting Bigquery to GCS spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data = spark.read \
            .format("bigquery") \
            .option("table", input_table) \
            .load()

        # Write
        if output_format == constants.BQ_GCS_OUTPUT_FORMAT_PARQUET:
            input_data.write \
                .mode(output_mode) \
                .parquet(output_location)
        elif output_format == constants.BQ_GCS_OUTPUT_FORMAT_AVRO:
            input_data.write \
                .mode(output_mode) \
                .format(constants.BQ_GCS_OUTPUT_FORMAT_AVRO) \
                .save(output_location)
        elif output_format == constants.BQ_GCS_OUTPUT_FORMAT_CSV:
            input_data.write \
                .mode(output_mode) \
                .option(constants.BQ_GCS_CSV_HEADER, True) \
                .csv(output_location)
        elif output_format == constants.BQ_GCS_OUTPUT_FORMAT_JSON:
            input_data.write \
                .mode(output_mode) \
                .json(output_location)
        else:
            raise Exception(
                "Currently avro, parquet, csv and json are the only supported formats"
            )
