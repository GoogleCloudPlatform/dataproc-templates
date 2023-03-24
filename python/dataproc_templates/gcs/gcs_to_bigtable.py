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

from pyspark.sql import SparkSession

from dataproc_templates import BaseTemplate
from dataproc_templates.util.argument_parsing import add_spark_options
from dataproc_templates.util.dataframe_reader_wrappers import ingest_dataframe_from_cloud_storage
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
            help='Cloud Storage location of the input files'
        )
        parser.add_argument(
            f'--{constants.GCS_BT_INPUT_FORMAT}',
            dest=constants.GCS_BT_INPUT_FORMAT,
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
        add_spark_options(parser, constants.get_csv_input_spark_options("gcs.bigtable.input."))
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
        input_location: str = args[constants.GCS_BT_INPUT_LOCATION]
        input_format: str = args[constants.GCS_BT_INPUT_FORMAT]
        catalog: str = ''.join(args[constants.GCS_BT_HBASE_CATALOG_JSON].split())

        logger.info(
            "Starting Cloud Storage to BigTable Spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data = ingest_dataframe_from_cloud_storage(
            spark, args, input_location, input_format, "gcs.bigtable.input."
        )

        # Write
        input_data.write \
            .format(constants.FORMAT_HBASE) \
            .options(catalog=catalog) \
            .option('hbase.spark.use.hbasecontext', "false") \
            .save()
