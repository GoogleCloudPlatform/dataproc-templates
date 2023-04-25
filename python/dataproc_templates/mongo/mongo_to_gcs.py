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


__all__ = ['MongoToGCSTemplate']


class MongoToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Mongo to Cloud Storage
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.MONGO_GCS_INPUT_URI}',
            dest=constants.MONGO_GCS_INPUT_URI,
            required=True,
            help='MONGO Cloud Storage Input Connection Uri'
        )
        parser.add_argument(
            f'--{constants.MONGO_GCS_INPUT_DATABASE}',
            dest=constants.MONGO_GCS_INPUT_DATABASE,
            required=True,
            help='MONGO Cloud Storage Input Database Name'
        )
        parser.add_argument(
            f'--{constants.MONGO_GCS_INPUT_COLLECTION}',
            dest=constants.MONGO_GCS_INPUT_COLLECTION,
            required=True,
            help='MONGO Cloud Storage Input Collection Name'
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
            help='Cloud Storage location for output files'
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
        add_spark_options(parser, constants.get_csv_output_spark_options("mongo.gcs.output."))

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

        ignore_keys = {constants.MONGO_GCS_INPUT_URI}
        filtered_args = {key:val for key,val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting Mongo to Cloud Storage Spark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
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
        persist_dataframe_to_cloud_storage(writer, args, output_location, output_format, "mongo.gcs.output.")