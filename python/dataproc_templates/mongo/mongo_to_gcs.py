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
import dataproc_templates.util.template_constants as constants

__all__ = ['MongoToGCSTemplate']


class MongoToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Mongo to GCS
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.MONGO_GCS_INPUT_URI}',
            dest=constants.MONGO_GCS_INPUT_URI,
            required=True,
            help='MONGO GCS Input Connection Uri'
        )
        parser.add_argument(
            f'--{constants.MONGO_GCS_INPUT_DATABASE}',
            dest=constants.MONGO_GCS_INPUT_DATABASE,
            required=True,
            help='MONGO GCS Input Database Name'
        )
        parser.add_argument(
            f'--{constants.MONGO_GCS_INPUT_COLLECTION}',
            dest=constants.MONGO_GCS_INPUT_COLLECTION,
            required=True,
            help='MONGO GCS Input Collection Name'
        )
        parser.add_argument(
            f'--{constants.MONGO_GCS_OUTPUT_FORMAT}',
            dest=constants.MONGO_GCS_OUTPUT_FORMAT,
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
            f'--{constants.MONGO_GCS_OUTPUT_LOCATION}',
            dest=constants.MONGO_GCS_OUTPUT_LOCATION,
            required=True,
            help='GCS location for output files'
        )
        parser.add_argument(
            f'--{constants.MONGO_GCS_OUTPUT_MODE}',
            dest=constants.MONGO_GCS_OUTPUT_MODE,
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

        # Arguments
        input_uri: str = args[constants.MONGO_GCS_INPUT_URI]
        input_database: str = args[constants.MONGO_GCS_INPUT_DATABASE]
        input_collection: str = args[constants.MONGO_GCS_INPUT_COLLECTION]
        output_format: str = args[constants.MONGO_GCS_OUTPUT_FORMAT]
        output_mode: str = args[constants.MONGO_GCS_OUTPUT_MODE]
        output_location: str = args[constants.MONGO_GCS_OUTPUT_LOCATION]

        logger.info(
            "Starting Mongo to GCS spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data = spark.read \
            .format(constants.FORMAT_MONGO) \
            .option(constants.MONGO_INPUT_URI, input_uri) \
            .option(constants.MONGO_DATABASE, input_database) \
            .option(constants.MONGO_COLLECTION, input_collection) \
            .load()

        # Write
        writer: DataFrameWriter = input_data.write.mode(output_mode)

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
