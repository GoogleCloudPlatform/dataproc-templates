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

__all__ = ['JDBCToJDBCTemplate']


class JDBCToJDBCTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from JDBC into JDBC
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.JDBCTOJDBC_INPUT_URL}',
            dest=constants.JDBCTOJDBC_INPUT_URL,
            required=True,
            help='JDBC input URL'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_INPUT_DRIVER}',
            dest=constants.JDBCTOJDBC_INPUT_DRIVER,
            required=True,
            help='JDBC input driver name'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_INPUT_TABLE}',
            dest=constants.JDBCTOJDBC_INPUT_TABLE,
            required=True,
            help='JDBC input table name'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_INPUT_PARTITIONCOLUMN}',
            dest=constants.JDBCTOJDBC_INPUT_PARTITIONCOLUMN,
            required=False,
            default="",
            help='JDBC input table partition column name'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_INPUT_LOWERBOUND}',
            dest=constants.JDBCTOJDBC_INPUT_LOWERBOUND,
            required=False,
            default="",
            help='JDBC input table partition column lower bound which is used to decide the partition stride'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_INPUT_UPPERBOUND}',
            dest=constants.JDBCTOJDBC_INPUT_UPPERBOUND,
            required=False,
            default="",
            help='JDBC input table partition column upper bound which is used to decide the partition stride'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_NUMPARTITIONS}',
            dest=constants.JDBCTOJDBC_NUMPARTITIONS,
            required=False,
            default="10",
            help='The maximum number of partitions that can be used for parallelism in table reading and writing. Default set to 10'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_OUTPUT_URL}',
            dest=constants.JDBCTOJDBC_OUTPUT_URL,
            required=True,
            help='JDBC output URL'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_OUTPUT_DRIVER}',
            dest=constants.JDBCTOJDBC_OUTPUT_DRIVER,
            required=True,
            help='JDBC output driver name'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_OUTPUT_TABLE}',
            dest=constants.JDBCTOJDBC_OUTPUT_TABLE,
            required=True,
            help='JDBC output table name'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_OUTPUT_CREATE_TABLE_OPTION}',
            dest=constants.JDBCTOJDBC_OUTPUT_CREATE_TABLE_OPTION,
            required=False,
            default="",
            help='This option allows setting of database-specific table and partition options when creating a output table'
        )
        parser.add_argument(
            f'--{constants.JDBCTOJDBC_OUTPUT_MODE}',
            dest=constants.JDBCTOJDBC_OUTPUT_MODE,
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
            f'--{constants.JDBCTOJDBC_OUTPUT_BATCH_SIZE}',
            dest=constants.JDBCTOJDBC_OUTPUT_BATCH_SIZE,
            required=False,
            default="1000",
            help='JDBC output batch size. Default set to 1000'
        )


        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_jdbc_url: str = args[constants.JDBCTOJDBC_INPUT_URL]
        input_jdbc_driver: str = args[constants.JDBCTOJDBC_INPUT_DRIVER]
        input_jdbc_table: str = args[constants.JDBCTOJDBC_INPUT_TABLE]
        input_jdbc_partitioncolumn: str = args[constants.JDBCTOJDBC_INPUT_PARTITIONCOLUMN]
        input_jdbc_lowerbound: str = args[constants.JDBCTOJDBC_INPUT_LOWERBOUND]
        input_jdbc_upperbound: str = args[constants.JDBCTOJDBC_INPUT_UPPERBOUND]
        jdbc_numpartitions: str = args[constants.JDBCTOJDBC_NUMPARTITIONS]
        output_jdbc_url: str = args[constants.JDBCTOJDBC_OUTPUT_URL]
        output_jdbc_driver: str = args[constants.JDBCTOJDBC_OUTPUT_DRIVER]
        output_jdbc_table: str = args[constants.JDBCTOJDBC_OUTPUT_TABLE]
        output_jdbc_create_table_option: str = args[constants.JDBCTOJDBC_OUTPUT_CREATE_TABLE_OPTION]
        output_jdbc_mode: str = args[constants.JDBCTOJDBC_OUTPUT_MODE]
        output_jdbc_batch_size: int = args[constants.JDBCTOJDBC_OUTPUT_BATCH_SIZE]


        logger.info(
            "Starting JDBC to JDBC spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )
        
        # Read
        input_data: DataFrame
        
        partition_parameters=str(input_jdbc_partitioncolumn) + str(input_jdbc_lowerbound) + str(input_jdbc_upperbound)
        if ((partition_parameters != "") & ((input_jdbc_partitioncolumn == "") | (input_jdbc_lowerbound == "") | (input_jdbc_upperbound == ""))):
            logger.error("Set all the sql partitioning parameters together-jdbctojdbc.input.partitioncolumn,jdbctojdbc.input.lowerbound,jdbctojdbc.input.upperbound. Refer to README.md for more instructions.")
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
        input_data.write \
            .format(constants.FORMAT_JDBC) \
            .option(constants.JDBC_URL, output_jdbc_url) \
            .option(constants.JDBC_DRIVER, output_jdbc_driver) \
            .option(constants.JDBC_TABLE, output_jdbc_table) \
            .option(constants.JDBC_CREATE_TABLE_OPTIONS, output_jdbc_create_table_option) \
            .option(constants.JDBC_BATCH_SIZE, output_jdbc_batch_size) \
            .option(constants.JDBC_NUMPARTITIONS, jdbc_numpartitions) \
            .mode(output_jdbc_mode) \
            .save()