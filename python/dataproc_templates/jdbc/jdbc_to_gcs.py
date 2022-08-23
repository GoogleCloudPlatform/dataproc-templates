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

__all__ = ['JDBCToGCSTemplate']


class JDBCToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from JDBC into GCS
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.JDBCTOGCS_INPUT_URL}',
            dest=constants.JDBCTOGCS_INPUT_URL,
            required=True,
            help='JDBC input URL'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_INPUT_DRIVER}',
            dest=constants.JDBCTOGCS_INPUT_DRIVER,
            required=True,
            help='JDBC input driver name'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_INPUT_TABLE}',
            dest=constants.JDBCTOGCS_INPUT_TABLE,
            required=True,
            help='JDBC input table name'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_INPUT_PARTITIONCOLUMN}',
            dest=constants.JDBCTOGCS_INPUT_PARTITIONCOLUMN,
            required=False,
            default="",
            help='JDBC input table partition column name'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_INPUT_LOWERBOUND}',
            dest=constants.JDBCTOGCS_INPUT_LOWERBOUND,
            required=False,
            default="",
            help='JDBC input table partition column lower bound which is used to decide the partition stride'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_INPUT_UPPERBOUND}',
            dest=constants.JDBCTOGCS_INPUT_UPPERBOUND,
            required=False,
            default="",
            help='JDBC input table partition column upper bound which is used to decide the partition stride'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_NUMPARTITIONS}',
            dest=constants.JDBCTOGCS_NUMPARTITIONS,
            required=False,
            default="10",
            help='The maximum number of partitions that can be used for parallelism in table reading and writing. Default set to 10'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_OUTPUT_LOCATION}',
            dest=constants.JDBCTOGCS_OUTPUT_LOCATION,
            required=True,
            help='GCS location for output files'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_OUTPUT_FORMAT}',
            dest=constants.JDBCTOGCS_OUTPUT_FORMAT,
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
            f'--{constants.JDBCTOGCS_OUTPUT_MODE}',
            dest=constants.JDBCTOGCS_OUTPUT_MODE,
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
            f'--{constants.JDBCTOGCS_OUTPUT_PARTITIONCOLUMN}',
            dest=constants.JDBCTOGCS_OUTPUT_PARTITIONCOLUMN,
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
        input_jdbc_url: str = args[constants.JDBCTOGCS_INPUT_URL]
        input_jdbc_driver: str = args[constants.JDBCTOGCS_INPUT_DRIVER]
        input_jdbc_table: str = args[constants.JDBCTOGCS_INPUT_TABLE]
        input_jdbc_partitioncolumn: str = args[constants.JDBCTOGCS_INPUT_PARTITIONCOLUMN]
        input_jdbc_lowerbound: str = args[constants.JDBCTOGCS_INPUT_LOWERBOUND]
        input_jdbc_upperbound: str = args[constants.JDBCTOGCS_INPUT_UPPERBOUND]
        jdbc_numpartitions: str = args[constants.JDBCTOGCS_NUMPARTITIONS]
        output_location: str = args[constants.JDBCTOGCS_OUTPUT_LOCATION]
        output_format: str = args[constants.JDBCTOGCS_OUTPUT_FORMAT]
        output_mode: str = args[constants.JDBCTOGCS_OUTPUT_MODE]
        output_partitioncolumn: str = args[constants.JDBCTOGCS_OUTPUT_PARTITIONCOLUMN]

        logger.info(
            "Starting JDBC to GCS spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )
        
        # Read
        input_data: DataFrame
        
        partition_parameters=str(input_jdbc_partitioncolumn) + str(input_jdbc_lowerbound) + str(input_jdbc_upperbound)
        if ((partition_parameters != "") & ((input_jdbc_partitioncolumn == "") | (input_jdbc_lowerbound == "") | (input_jdbc_upperbound == ""))):
            logger.error("Set all the sql partitioning parameters together-jdbctogcs.input.partitioncolumn,jdbctogcs.input.lowerbound,jdbctogcs.input.upperbound. Refer to README.md for more instructions.")
            exit (1)
        elif (partition_parameters == ""):
            input_data=spark.read \
                .format(constants.FORMAT_JDBC) \
                .option(constants.JDBC_URL, input_jdbc_url) \
                .option(constants.JDBC_DRIVER, input_jdbc_driver) \
                .option(constants.JDBC_TABLE, input_jdbc_table) \
                .option(constants.JDBC_NUMPARTITIONS, jdbc_numpartitions) \
                .load()
        else:
            input_data=spark.read \
                .format(constants.FORMAT_JDBC) \
                .option(constants.JDBC_URL, input_jdbc_url) \
                .option(constants.JDBC_DRIVER, input_jdbc_driver) \
                .option(constants.JDBC_TABLE, input_jdbc_table) \
                .option(constants.JDBC_PARTITIONCOLUMN, input_jdbc_partitioncolumn) \
                .option(constants.JDBC_LOWERBOUND, input_jdbc_lowerbound) \
                .option(constants.JDBC_UPPERBOUND, input_jdbc_upperbound) \
                .option(constants.JDBC_NUMPARTITIONS, jdbc_numpartitions) \
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