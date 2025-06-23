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


__all__ = ['BigQueryToMemorystoreTemplate']


class BigQueryToMemorystoreTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from BigQuery to Memorystore
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.BQ_MEMORYSTORE_INPUT_TABLE}',
            dest=constants.BQ_MEMORYSTORE_INPUT_TABLE,
            required=True,
            help='BigQuery Input table name'
        )
        parser.add_argument(
            f'--{constants.BQ_MEMORYSTORE_OUTPUT_HOST}',
            dest=constants.BQ_MEMORYSTORE_OUTPUT_HOST,
            required=True,
            help='Redis Memorystore host',
        )
        parser.add_argument(
            f'--{constants.BQ_MEMORYSTORE_OUTPUT_PORT}',
            dest=constants.BQ_MEMORYSTORE_OUTPUT_PORT,
            required=False,
            default=6379,
            help='Redis Memorystore port. Defaults to 6379',
        )
        #todo: add defaults
        parser.add_argument(
            f'--{constants.BQ_MEMORYSTORE_OUTPUT_TABLE}',
            dest=constants.BQ_MEMORYSTORE_OUTPUT_TABLE,
            required=True,
            help='Redis Memorystore target table name',
        )
        parser.add_argument(
            f'--{constants.BQ_MEMORYSTORE_OUTPUT_KEY_COLUMN}',
            dest=constants.BQ_MEMORYSTORE_OUTPUT_KEY_COLUMN,
            required=True,
            help='Redis Memorystore key column for target table',
        )
        parser.add_argument(
            f'--{constants.BQ_MEMORYSTORE_OUTPUT_MODEL}',
            dest=constants.BQ_MEMORYSTORE_OUTPUT_MODEL,
            required=False,
            default=constants.BQ_MEMORYSTORE_OUTPUT_MODEL_HASH,
            help=(
                'Memorystore persistence model for Dataframe'
                '(one of: hash, binary) '
                '(Defaults to hash)'
            ),
            choices=[
                constants.BQ_MEMORYSTORE_OUTPUT_MODEL_HASH,
                constants.BQ_MEMORYSTORE_OUTPUT_MODEL_BINARY
            ]
        )
        parser.add_argument(
            f'--{constants.BQ_MEMORYSTORE_OUTPUT_MODE}',
            dest=constants.BQ_MEMORYSTORE_OUTPUT_MODE,
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
            f'--{constants.BQ_MEMORYSTORE_OUTPUT_TTL}',
            dest=constants.BQ_MEMORYSTORE_OUTPUT_TTL,
            required=False,
            default=0,
            help=(
                'Data time to live in seconds. Data doesn\'t expire if ttl is less than 1'
                '(Defaults to 0)'
            )
        )
        parser.add_argument(
            f'--{constants.BQ_MEMORYSTORE_OUTPUT_DBNUM}',
            dest=constants.BQ_MEMORYSTORE_OUTPUT_DBNUM,
            required=False,
            default=0,
            help=(
                'Database / namespace for logical key separation'
                '(Defaults to 0)'
            )
        )
        
        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_table: str = args[constants.BQ_MEMORYSTORE_INPUT_TABLE]

        output_table: str = args[constants.BQ_MEMORYSTORE_OUTPUT_TABLE]
        host: str = args[constants.BQ_MEMORYSTORE_OUTPUT_HOST]
        port: str = args[constants.BQ_MEMORYSTORE_OUTPUT_PORT]
        key_column: str = args[constants.BQ_MEMORYSTORE_OUTPUT_KEY_COLUMN]
        model: str = args[constants.BQ_MEMORYSTORE_OUTPUT_MODEL]
        ttl: int = args[constants.BQ_MEMORYSTORE_OUTPUT_TTL]
        dbnum: int = args[constants.BQ_MEMORYSTORE_OUTPUT_DBNUM]
        output_mode: str = args[constants.BQ_MEMORYSTORE_OUTPUT_MODE]

        logger.info(
            "Starting Bigquery to Memorystore Spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data = spark.read \
            .format(constants.FORMAT_BIGQUERY) \
            .option(constants.TABLE, input_table) \
            .load()

        # Write
        input_data.write \
            .format(constants.FORMAT_MEMORYSTORE) \
            .option(constants.TABLE, output_table) \
            .option(constants.MEMORYSTORE_KEY_COLUMN, key_column) \
            .option(constants.MEMORYSTORE_MODEL, model) \
            .option(constants.MEMORYSTORE_HOST, host) \
            .option(constants.MEMORYSTORE_PORT, port) \
            .option(constants.MEMORYSTORE_DBNUM, dbnum) \
            .option(constants.MEMORYSTORE_TTL, ttl) \
            .mode(output_mode) \
            .save()
