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
import dataproc_templates.util.secret_manager as secret_manager

__all__ = ['JDBCToBigQueryTemplate']


class JDBCToBigQueryTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from JDBC into BigQuery
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.JDBC_BQ_OUTPUT_DATASET}',
            dest=constants.JDBC_BQ_OUTPUT_DATASET,
            required=True,
            help='BigQuery dataset for the output table'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_OUTPUT_TABLE}',
            dest=constants.JDBC_BQ_OUTPUT_TABLE,
            required=True,
            help='BigQuery output table name'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_LD_TEMP_BUCKET_NAME}',
            dest=constants.JDBC_BQ_LD_TEMP_BUCKET_NAME,
            required=True,
            help='Spark BigQuery connector temporary bucket'
        )

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            f'--{constants.JDBC_BQ_INPUT_URL}',
            dest=constants.JDBC_BQ_INPUT_URL,
            required=False,
            default="",
            help='JDBC input URL'
        )
        group.add_argument(
            f'--{constants.JDBC_BQ_INPUT_URL_SECRET}',
            dest=constants.JDBC_BQ_INPUT_URL_SECRET,
            required=False,
            default="",
            help='JDBC input URL secret name'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_INPUT_DRIVER}',
            dest=constants.JDBC_BQ_INPUT_DRIVER,
            required=True,
            help='JDBC input driver name'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_INPUT_TABLE}',
            dest=constants.JDBC_BQ_INPUT_TABLE,
            required=True,
            help='JDBC input table name'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_INPUT_PARTITIONCOLUMN}',
            dest=constants.JDBC_BQ_INPUT_PARTITIONCOLUMN,
            required=False,
            default="",
            help='JDBC input table partition column name'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_INPUT_LOWERBOUND}',
            dest=constants.JDBC_BQ_INPUT_LOWERBOUND,
            required=False,
            default="",
            help='JDBC input table partition column lower bound which is used to decide the partition stride'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_INPUT_UPPERBOUND}',
            dest=constants.JDBC_BQ_INPUT_UPPERBOUND,
            required=False,
            default="",
            help='JDBC input table partition column upper bound which is used to decide the partition stride'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_NUMPARTITIONS}',
            dest=constants.JDBC_BQ_NUMPARTITIONS,
            required=False,
            default="10",
            help='The maximum number of partitions that can be used for parallelism in table reading and writing. Default set to 10'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_INPUT_FETCHSIZE}',
            dest=constants.JDBC_BQ_INPUT_FETCHSIZE,
            required=False,
            default=0,
            type=int,
            help='Determines how many rows to fetch per round trip'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_SESSIONINITSTATEMENT}',
            dest=constants.JDBC_BQ_SESSIONINITSTATEMENT,
            required=False,
            default="",
            help='Custom SQL statement to execute in each reader database session'
        )
        parser.add_argument(
            f'--{constants.JDBC_BQ_OUTPUT_MODE}',
            dest=constants.JDBC_BQ_OUTPUT_MODE,
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
        big_query_dataset: str = args[constants.JDBC_BQ_OUTPUT_DATASET]
        big_query_table: str = args[constants.JDBC_BQ_OUTPUT_TABLE]
        bq_temp_bucket: str = args[constants.JDBC_BQ_LD_TEMP_BUCKET_NAME]
        
        #check if secret is passed or the connection string in the agruments
        if str(args[constants.JDBC_BQ_INPUT_URL])=="":
            input_jdbc_url: str = secret_manager.access_secret_version(args[constants.JDBC_BQ_INPUT_URL_SECRET])
        else:
            input_jdbc_url: str = args[constants.JDBC_BQ_INPUT_URL]

        input_jdbc_driver: str = args[constants.JDBC_BQ_INPUT_DRIVER]
        input_jdbc_table: str = args[constants.JDBC_BQ_INPUT_TABLE]
        input_jdbc_partitioncolumn: str = args[constants.JDBC_BQ_INPUT_PARTITIONCOLUMN]
        input_jdbc_lowerbound: str = args[constants.JDBC_BQ_INPUT_LOWERBOUND]
        input_jdbc_upperbound: str = args[constants.JDBC_BQ_INPUT_UPPERBOUND]
        jdbc_numpartitions: str = args[constants.JDBC_BQ_NUMPARTITIONS]
        input_jdbc_fetchsize: int = args[constants.JDBC_BQ_INPUT_FETCHSIZE]
        input_jdbc_sessioninitstatement: str = args[constants.JDBC_BQ_SESSIONINITSTATEMENT]
        output_mode: str = args[constants.JDBC_BQ_OUTPUT_MODE]

        ignore_keys = {constants.JDBC_BQ_INPUT_URL}
        filtered_args = {key:val for key,val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting JDBC to BigQuery Spark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
        )

        # Read
        input_data: DataFrame

        partition_parameters = str(input_jdbc_partitioncolumn) + str(input_jdbc_lowerbound) + str(input_jdbc_upperbound)
        if ((partition_parameters != "") & ((input_jdbc_partitioncolumn == "") | (input_jdbc_lowerbound == "") | (input_jdbc_upperbound == ""))):
            logger.error("Set all the sql partitioning parameters together-jdbctogcs.input.partitioncolumn,jdbctogcs.input.lowerbound,jdbctogcs.input.upperbound. Refer to README.md for more instructions.")
            exit (1)

        properties = {constants.JDBC_URL: input_jdbc_url,
                      constants.JDBC_DRIVER: input_jdbc_driver,
                      constants.JDBC_TABLE: input_jdbc_table,
                      constants.JDBC_NUMPARTITIONS: jdbc_numpartitions,
                      constants.JDBC_FETCHSIZE: input_jdbc_fetchsize}
        if input_jdbc_sessioninitstatement:
            properties[constants.JDBC_SESSIONINITSTATEMENT] = input_jdbc_sessioninitstatement
        if partition_parameters:
            properties.update({constants.JDBC_PARTITIONCOLUMN: input_jdbc_partitioncolumn,
                               constants.JDBC_LOWERBOUND: input_jdbc_lowerbound,
                               constants.JDBC_UPPERBOUND: input_jdbc_upperbound})

        input_data = spark.read \
            .format(constants.FORMAT_JDBC) \
            .options(**properties) \
            .load()

        # Write
        input_data.write \
                .format(constants.FORMAT_BIGQUERY) \
                .option(constants.TABLE, big_query_dataset + "." + big_query_table) \
                .option(constants.GCS_BQ_TEMP_BUCKET, bq_temp_bucket) \
                .mode(output_mode) \
                .save()

