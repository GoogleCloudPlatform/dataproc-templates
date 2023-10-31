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


__all__ = ['MongoToBigQueryTemplate']


class MongoToBigQueryTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Mongo to BigQuery
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.MONGO_BQ_INPUT_URI}',
            dest=constants.MONGO_BQ_INPUT_URI,
            required=True,
            help='Mongo Input Connection Uri'
        )
        parser.add_argument(
            f'--{constants.MONGO_BQ_INPUT_DATABASE}',
            dest=constants.MONGO_BQ_INPUT_DATABASE,
            required=True,
            help='Mongo Input Database Name'
        )
        parser.add_argument(
            f'--{constants.MONGO_BQ_INPUT_COLLECTION}',
            dest=constants.MONGO_BQ_INPUT_COLLECTION,
            required=True,
            help='Mongo Input Collection Name'
        )

        parser.add_argument(
            f'--{constants.MONGO_BQ_OUTPUT_DATASET}',
            dest=constants.MONGO_BQ_OUTPUT_DATASET,
            required=True,
            help='BigQuery Output Dataset Name'
        )

        parser.add_argument(
            f'--{constants.MONGO_BQ_OUTPUT_TABLE}',
            dest=constants.MONGO_BQ_OUTPUT_TABLE,
            required=True,
            help='BigQuery Output Table Name'
        )

        parser.add_argument(
            f'--{constants.MONGO_BQ_OUTPUT_MODE}',
            dest=constants.MONGO_BQ_OUTPUT_MODE,
            required=False,
            default=constants.OUTPUT_MODE_APPEND,
            help=(
                'BigQuery Output write mode '
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
            f'--{constants.MONGO_BQ_TEMP_BUCKET_NAME}',
            dest=constants.MONGO_BQ_TEMP_BUCKET_NAME,
            required=True,
            help='GCS Temp Bucket Name'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_uri: str = args[constants.MONGO_BQ_INPUT_URI]
        input_database: str = args[constants.MONGO_BQ_INPUT_DATABASE]
        input_collection: str = args[constants.MONGO_BQ_INPUT_COLLECTION]
        output_mode: str = args[constants.MONGO_BQ_OUTPUT_MODE]
        big_query_output_dataset: str = args[constants.MONGO_BQ_OUTPUT_DATASET]
        big_query_output_table: str = args[constants.MONGO_BQ_OUTPUT_TABLE]
        big_query_temp_bucket: str = args[constants.MONGO_BQ_TEMP_BUCKET_NAME]

        ignore_keys = {constants.MONGO_BQ_INPUT_URI}
        filtered_args = {key: val for key,
                         val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting Mongo to Big Query Spark job with parameters:\n"
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
        input_data.write \
                .format(constants.FORMAT_BIGQUERY) \
                .option(constants.TABLE, big_query_output_dataset + "." + big_query_output_table) \
                .option(constants.TEMP_GCS_BUCKET, big_query_temp_bucket) \
                .mode(output_mode) \
                .save()
