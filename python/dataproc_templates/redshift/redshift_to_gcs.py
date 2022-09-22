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

__all__ = ['RedshiftToGCSTemplate']


class RedshiftToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from REDSHIFT into GCS
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.REDSHIFTTOGCS_INPUT_URL}',
            dest=constants.REDSHIFTTOGCS_INPUT_URL,
            required=True,
            help='REDSHIFT input URL'
        )
        parser.add_argument(
            f'--{constants.REDSHIFTTOGCS_INPUT_TABLE}',
            dest=constants.REDSHIFTTOGCS_INPUT_TABLE,
            required=True,
            help='REDSHIFT input table name'
        )
        parser.add_argument(
            f'--{constants.REDSHIFTTOGCS_S3_TEMPDIR}',
            dest=constants.REDSHIFTTOGCS_S3_TEMPDIR,
            required=True,
            help='REDSHIFT S3 temporary bucket location s3a://bucket/path'
        )
        parser.add_argument(
            f'--{constants.REDSHIFTTOGCS_IAM_ROLEARN}',
            dest=constants.REDSHIFTTOGCS_IAM_ROLEARN,
            required=True,
            help='REDSHIFT IAM Role with S3 Access'
        )
        parser.add_argument(
            f'--{constants.REDSHIFTTOGCS_S3_ACCESSKEY}',
            dest=constants.REDSHIFTTOGCS_S3_ACCESSKEY,
            required=True,
            default="",
            help='AWS Access Keys which allow access to S3'
        )
        parser.add_argument(
            f'--{constants.REDSHIFTTOGCS_S3_SECRETKEY}',
            dest=constants.REDSHIFTTOGCS_S3_SECRETKEY,
            required=True,
            default="",
            help='AWS Secret Keys which allow access to S3'
        )
        parser.add_argument(
            f'--{constants.REDSHIFTTOGCS_OUTPUT_LOCATION}',
            dest=constants.REDSHIFTTOGCS_OUTPUT_LOCATION,
            required=True,
            help='GCS location for output files'
        )
        parser.add_argument(
            f'--{constants.REDSHIFTTOGCS_OUTPUT_FORMAT}',
            dest=constants.REDSHIFTTOGCS_OUTPUT_FORMAT,
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
            f'--{constants.REDSHIFTTOGCS_OUTPUT_MODE}',
            dest=constants.REDSHIFTTOGCS_OUTPUT_MODE,
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
            f'--{constants.REDSHIFTTOGCS_OUTPUT_PARTITIONCOLUMN}',
            dest=constants.REDSHIFTTOGCS_OUTPUT_PARTITIONCOLUMN,
            required=False,
            default="",
            help='GCS partition column name'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_redshift_url: str = args[constants.REDSHIFTTOGCS_INPUT_URL]
        input_redshift_tempdir: str = args[constants.REDSHIFTTOGCS_S3_TEMPDIR]
        input_redshift_table: str = args[constants.REDSHIFTTOGCS_INPUT_TABLE]
        input_redshift_iam_role: str = args[constants.REDSHIFTTOGCS_IAM_ROLEARN]
        input_redshift_accesskey: str = args[constants.REDSHIFTTOGCS_S3_ACCESSKEY]
        input_redshift_secretkey: str = args[constants.REDSHIFTTOGCS_S3_SECRETKEY]
        
        output_location: str = args[constants.REDSHIFTTOGCS_OUTPUT_LOCATION]
        output_format: str = args[constants.REDSHIFTTOGCS_OUTPUT_FORMAT]
        output_mode: str = args[constants.REDSHIFTTOGCS_OUTPUT_MODE]
        output_partitioncolumn: str = args[constants.REDSHIFTTOGCS_OUTPUT_PARTITIONCOLUMN]

        logger.info(
            "Starting REDSHIFT to GCS spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )
        
        # Read
        input_data: DataFrame
        
        spark._jsc.hadoopConfiguration().set(constants.AWS_S3ACCESSKEY, input_redshift_accesskey)
        spark._jsc.hadoopConfiguration().set(constants.AWS_S3SECRETKEY, input_redshift_secretkey)
        
        input_data=spark.read \
                .format(constants.FORMAT_REDSHIFT) \
                .option(constants.JDBC_URL, input_redshift_url) \
                .option(constants.JDBC_TABLE, input_redshift_table) \
                .option(constants.REDSHIFT_TEMPDIR, input_redshift_tempdir) \
                .option(constants.REDSHIFT_IAMROLE, input_redshift_iam_role) \
                .load()

        # Write
        if (output_partitioncolumn != ""):
            writer: DataFrameWriter = input_data.write.mode(output_mode).partitionBy(output_partitioncolumn)
        else:
            writer: DataFrameWriter = input_data.write.mode(output_mode)
            
        if output_format == constants.FORMAT_PRQT:
            writer \
                .parquet(output_location)
        elif output_format == constants.FORMAT_AVRO:
            writer \
                .format(constants.FORMAT_AVRO) \
                .save(output_location)
        elif output_format == constants.FORMAT_CSV:
            writer \
                .option(constants.HEADER, True) \
                .csv(output_location)
        elif output_format == constants.FORMAT_JSON:
            writer \
                .json(output_location)
