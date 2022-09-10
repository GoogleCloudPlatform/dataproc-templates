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

from pyspark.sql import SparkSession, DataFrame

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['GCSToBigTableTemplate']


class GCSToBigTableTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from GCS into BigTable
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.GCS_BT_INPUT_LOCATION}',
            dest=constants.GCS_BT_INPUT_LOCATION,
            required=True,
            help='GCS location of the input files'
        )
        parser.add_argument(
            f'--{constants.GCS_BT_INPUT_FORMAT}',
            dest=constants.GCS_BT_INPUT_FORMAT,
            required=True,
            help='Input file format (one of: avro,parquet,csv,json)',
            choices=[
                constants.FORMAT_AVRO,
                constants.FORMAT_PRQT,
                constants.FORMAT_CSV,
                constants.FORMAT_JSON
            ]
        )
        parser.add_argument(
            f'--{constants.GCS_BT_HBASE_CATALOG_JSON}',
            dest=constants.GCS_BT_HBASE_CATALOG_JSON,
            required=True,
            help='HBase catalog inline json'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_file_location: str = args[constants.GCS_BT_INPUT_LOCATION]
        input_file_format: str = args[constants.GCS_BT_INPUT_FORMAT]
        catalog: str = ''.join(args[constants.GCS_BT_HBASE_CATALOG_JSON].split())

        logger.info(
            "Starting GCS to BigTable spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data: DataFrame

        if input_file_format == constants.FORMAT_PRQT:
            input_data = spark.read \
                .parquet(input_file_location)
        elif input_file_format == constants.FORMAT_AVRO:
            input_data = spark.read \
                .format(constants.FORMAT_AVRO_EXTD) \
                .load(input_file_location)
        elif input_file_format == constants.FORMAT_CSV:
            input_data = spark.read \
                .format(constants.FORMAT_CSV) \
                .option(constants.HEADER, True) \
                .option(constants.INFER_SCHEMA, True) \
                .load(input_file_location)
        elif input_file_format == constants.FORMAT_JSON:
            input_data = spark.read \
                .json(input_file_location)

        # Write
        input_data.write \
            .format(constants.FORMAT_HBASE) \
            .options(catalog=catalog) \
            .option('hbase.spark.use.hbasecontext', "false") \
            .save()
