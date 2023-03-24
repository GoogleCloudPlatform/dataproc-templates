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


__all__ = ['GCSToBigQueryTemplate']


class GCSToBigQueryTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from GCS into BigQuery
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.GCS_BQ_INPUT_LOCATION}',
            dest=constants.GCS_BQ_INPUT_LOCATION,
            required=True,
            help='Cloud Storage location of the input files'
        )
        parser.add_argument(
            f'--{constants.GCS_BQ_OUTPUT_DATASET}',
            dest=constants.GCS_BQ_OUTPUT_DATASET,
            required=True,
            help='BigQuery dataset for the output table'
        )
        parser.add_argument(
            f'--{constants.GCS_BQ_OUTPUT_TABLE}',
            dest=constants.GCS_BQ_OUTPUT_TABLE,
            required=True,
            help='BigQuery output table name'
        )
        parser.add_argument(
            f'--{constants.GCS_BQ_INPUT_FORMAT}',
            dest=constants.GCS_BQ_INPUT_FORMAT,
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
        add_spark_options(parser, constants.get_csv_input_spark_options("gcs.bigquery.input."))
        parser.add_argument(
            f'--{constants.GCS_BQ_LD_TEMP_BUCKET_NAME}',
            dest=constants.GCS_BQ_LD_TEMP_BUCKET_NAME,
            required=True,
            help='Spark BigQuery connector temporary bucket'
        )
        parser.add_argument(
            f'--{constants.GCS_BQ_OUTPUT_MODE}',
            dest=constants.GCS_BQ_OUTPUT_MODE,
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
        input_location: str = args[constants.GCS_BQ_INPUT_LOCATION]
        input_format: str = args[constants.GCS_BQ_INPUT_FORMAT]
        big_query_dataset: str = args[constants.GCS_BQ_OUTPUT_DATASET]
        big_query_table: str = args[constants.GCS_BQ_OUTPUT_TABLE]
        bq_temp_bucket: str = args[constants.GCS_BQ_LD_TEMP_BUCKET_NAME]
        output_mode: str = args[constants.GCS_BQ_OUTPUT_MODE]

        logger.info(
            "Starting Cloud Storage to BigQuery Spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data = ingest_dataframe_from_cloud_storage(
            spark, args, input_location, input_format, "gcs.bigquery.input."
        )

        # Write
        input_data.write \
            .format(constants.FORMAT_BIGQUERY) \
            .option(constants.TABLE, big_query_dataset + "." + big_query_table) \
            .option(constants.GCS_BQ_TEMP_BUCKET, bq_temp_bucket) \
            .mode(output_mode) \
            .save()
