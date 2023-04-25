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
from pyspark.sql import SparkSession

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants
from dataproc_templates.util.argument_parsing import add_spark_options
from dataproc_templates.util.dataframe_reader_wrappers import ingest_dataframe_from_cloud_storage


__all__ = ['GCSToMONGOTemplate']


class GCSToMONGOTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from GCS into MongoDB Database
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.GCS_MONGO_INPUT_LOCATION}',
            dest=constants.GCS_MONGO_INPUT_LOCATION,
            required=True,
            help='Cloud Storage location of the input files'
        )
        parser.add_argument(
            f'--{constants.GCS_MONGO_INPUT_FORMAT}',
            dest=constants.GCS_MONGO_INPUT_FORMAT,
            required=True,
            help='Input file format (one of: avro,parquet,csv,json,delta)',
            choices=[
                constants.FORMAT_AVRO,
                constants.FORMAT_PRQT,
                constants.FORMAT_CSV,
                constants.FORMAT_JSON,
                constants.FORMAT_DELTA
            ]
        )
        add_spark_options(parser, constants.get_csv_input_spark_options("gcs.mongo.input."))
        parser.add_argument(
            f'--{constants.GCS_MONGO_OUTPUT_URI}',
            dest=constants.GCS_MONGO_OUTPUT_URI,
            required=True,
            help='GCS MONGO Output Connection Uri'
        )
        parser.add_argument(
            f'--{constants.GCS_MONGO_OUTPUT_DATABASE}',
            dest=constants.GCS_MONGO_OUTPUT_DATABASE,
            required=True,
            help='GCS MONGO Output Database Name'
        )
        parser.add_argument(
            f'--{constants.GCS_MONGO_OUTPUT_COLLECTION}',
            dest=constants.GCS_MONGO_OUTPUT_COLLECTION,
            required=True,
            help='GCS MONGO Output Collection Name'
        )
        parser.add_argument(
            f'--{constants.GCS_MONGO_OUTPUT_MODE}',
            dest=constants.GCS_MONGO_OUTPUT_MODE,
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
            f'--{constants.GCS_MONGO_BATCH_SIZE}',
            dest=constants.GCS_MONGO_BATCH_SIZE,
            required=False,
            default=constants.MONGO_DEFAULT_BATCH_SIZE,
            help='GCS MONGO Output Batch Size'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_location: str = args[constants.GCS_MONGO_INPUT_LOCATION]
        input_format: str = args[constants.GCS_MONGO_INPUT_FORMAT]
        output_uri:str = args[constants.GCS_MONGO_OUTPUT_URI]
        output_database:str = args[constants.GCS_MONGO_OUTPUT_DATABASE]
        output_collection:str = args[constants.GCS_MONGO_OUTPUT_COLLECTION]
        output_mode:str = args[constants.GCS_MONGO_OUTPUT_MODE]
        batch_size:int = args[constants.GCS_MONGO_BATCH_SIZE]

        ignore_keys = {constants.GCS_MONGO_OUTPUT_URI}
        filtered_args = {key:val for key,val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting GCS to MONGO spark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
        )

        # Read
        input_data = ingest_dataframe_from_cloud_storage(spark, args, input_location, input_format, "gcs.mongo.input.")

        # Write
        input_data.write.format(constants.FORMAT_MONGO)\
            .option(constants.MONGO_URL, output_uri) \
            .option(constants.MONGO_DATABASE, output_database) \
            .option(constants.MONGO_COLLECTION, output_collection) \
            .option(constants.MONGO_BATCH_SIZE, batch_size) \
            .mode(output_mode) \
            .save()



