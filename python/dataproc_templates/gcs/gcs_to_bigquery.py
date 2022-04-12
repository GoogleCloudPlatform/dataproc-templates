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

__all__ = ['GcsToBigQueryTemplate']


class GcsToBigQueryTemplate(BaseTemplate):

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.GCS_BQ_INPUT_LOCATION}',
            dest=constants.GCS_BQ_INPUT_LOCATION,
            required=True,
            help='GCS location of the input files'
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
            help='Input file format (one of: avro,parquet,csv)',
            choices=[
                constants.GCS_BQ_AVRO_FORMAT,
                constants.GCS_BQ_PRQT_FORMAT,
                constants.GCS_BQ_CSV_FORMAT
            ]
        )
        parser.add_argument(
            f'--{constants.GCS_BQ_LD_TEMP_BUCKET_NAME}',
            dest=constants.GCS_BQ_LD_TEMP_BUCKET_NAME,
            required=True,
            help='Spark BigQuery connector temporary bucket'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)
    
    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        log4jLogger = spark.sparkContext._jvm.org.apache.log4j
        logger = log4jLogger.LogManager.getLogger(__name__)

        # Arguments
        input_file_location = args[constants.GCS_BQ_INPUT_LOCATION]
        big_query_dataset = args[constants.GCS_BQ_OUTPUT_DATASET]
        big_query_table = args[constants.GCS_BQ_OUTPUT_TABLE]
        input_file_format = args[constants.GCS_BQ_INPUT_FORMAT]
        bq_temp_bucket = args[constants.GCS_BQ_LD_TEMP_BUCKET_NAME]

        logger.info(
            "Starting GCS to Bigquery spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        if (input_file_format == constants.GCS_BQ_PRQT_FORMAT):
            input_data = spark.read \
                                .parquet(input_file_location)
        elif (input_file_format == constants.GCS_BQ_AVRO_FORMAT):
            input_data = spark.read \
                                .format(constants.GCS_BQ_AVRO_EXTD_FORMAT) \
                                .load(input_file_location)
        elif (input_file_format == constants.GCS_BQ_CSV_FORMAT):
            input_data = spark.read \
                                .format(constants.GCS_BQ_CSV_FORMAT) \
                                .option(constants.GCS_BQ_CSV_HEADER, True) \
                                .option(constants.GCS_BQ_CSV_INFER_SCHEMA, True) \
                                .load(input_file_location)

        # Write
        input_data.write \
                .format(constants.GCS_BQ_OUTPUT_FORMAT) \
                .option(constants.GCS_BQ_OUTPUT, big_query_dataset + "." + big_query_table) \
                .option(constants.GCS_BQ_TEMP_BUCKET, bq_temp_bucket) \
                .mode("append") \
                .save()
