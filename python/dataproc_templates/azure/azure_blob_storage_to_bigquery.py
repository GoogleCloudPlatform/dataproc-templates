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
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants


__all__ = ['AzureBlobStorageToBigQueryTemplate']


class AzureBlobStorageToBigQueryTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from GCS into BigQuery
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.AZ_BLOB_BQ_INPUT_LOCATION}',
            dest=constants.AZ_BLOB_BQ_INPUT_LOCATION,
            required=True,
            help='Location of the input files'
        )
        parser.add_argument(
            f'--{constants.AZ_BLOB_BQ_OUTPUT_DATASET}',
            dest=constants.AZ_BLOB_BQ_OUTPUT_DATASET,
            required=True,
            help='BigQuery dataset for the output table'
        )
        parser.add_argument(
            f'--{constants.AZ_BLOB_BQ_OUTPUT_TABLE}',
            dest=constants.AZ_BLOB_BQ_OUTPUT_TABLE,
            required=True,
            help='BigQuery output table name'
        )
        parser.add_argument(
            f'--{constants.AZ_BLOB_BQ_INPUT_FORMAT}',
            dest=constants.AZ_BLOB_BQ_INPUT_FORMAT,
            required=True,
            help='Input file format (one of: avro,parquet,csv,json)',
            choices=[
                constants.FORMAT_AVRO,
                constants.FORMAT_PRQT,
                constants.FORMAT_CSV,
                constants.FORMAT_JSON,
                constants.FORMAT_DELTA
            ]
        )
        parser.add_argument(
            f'--{constants.AZ_BLOB_BQ_LD_TEMP_BUCKET_NAME}',
            dest=constants.AZ_BLOB_BQ_LD_TEMP_BUCKET_NAME,
            required=True,
            help='Spark BigQuery connector temporary bucket'
        )
        parser.add_argument(
            f'--{constants.AZ_BLOB_BQ_OUTPUT_MODE}',
            dest=constants.AZ_BLOB_BQ_OUTPUT_MODE,
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
            f'--{constants.AZ_BLOB_STORAGE_ACCOUNT}',
            dest=constants.AZ_BLOB_STORAGE_ACCOUNT,
            required=True,
            help='Azure Storage Account'
        )
        parser.add_argument(
            f'--{constants.AZ_BLOB_CONTAINER_NAME}',
            dest=constants.AZ_BLOB_CONTAINER_NAME,
            required=True,
            help='Azure Account Name'
        )

        parser.add_argument(
            f'--{constants.AZ_BLOB_SAS_TOKEN}',
            dest=constants.AZ_BLOB_SAS_TOKEN,
            required=True,
            help='Azure SAS TOKEN'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_file_location: str = args[constants.AZ_BLOB_BQ_INPUT_LOCATION]
        big_query_dataset: str = args[constants.AZ_BLOB_BQ_OUTPUT_DATASET]
        big_query_table: str = args[constants.AZ_BLOB_BQ_OUTPUT_TABLE]
        input_file_format: str = args[constants.AZ_BLOB_BQ_INPUT_FORMAT]
        bq_temp_bucket: str = args[constants.AZ_BLOB_BQ_LD_TEMP_BUCKET_NAME]
        output_mode: str = args[constants.AZ_BLOB_BQ_OUTPUT_MODE]
        storage_account: str = args[constants.AZ_BLOB_STORAGE_ACCOUNT]
        container_name: str = args[constants.AZ_BLOB_CONTAINER_NAME]
        sas_token: str = args[constants.AZ_BLOB_SAS_TOKEN]

        spark.conf.set(f"fs.azure.sas.{container_name}.{storage_account}.blob.core.windows.net", sas_token)

        ignore_keys = {constants.AZ_BLOB_SAS_TOKEN}
        filtered_args = {key:val for key,val in args.items() if key not in ignore_keys}
        logger.info(f"Starting Azure to BigQuery spark job with parameters:\n {pprint.pformat(filtered_args)}")

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
        elif input_file_format == constants.FORMAT_DELTA:
            input_data = spark.read \
                .format(constants.FORMAT_DELTA) \
                .load(input_file_location)

        # Write
        input_data.write \
            .format(constants.FORMAT_BIGQUERY) \
            .option(constants.TABLE, big_query_dataset + "." + big_query_table) \
            .option(constants.AZ_BLOB_BQ_TEMP_BUCKET, bq_temp_bucket) \
            .mode(output_mode) \
            .save()
