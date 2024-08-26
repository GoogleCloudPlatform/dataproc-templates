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
import sys
from pyspark.sql import SparkSession, DataFrame, DataFrameWriter

from dataproc_templates import BaseTemplate
from dataproc_templates.util.argument_parsing import add_spark_options
from dataproc_templates.util.dataframe_writer_wrappers import persist_dataframe_to_cloud_storage
import dataproc_templates.util.template_constants as constants
import dataproc_templates.util.secret_manager as secret_manager


__all__ = ['JDBCToGCSTemplate']


class JDBCToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from JDBC into Cloud Storage
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            f'--{constants.JDBCTOGCS_INPUT_URL}',
            dest=constants.JDBCTOGCS_INPUT_URL,
            required=False,
            default="",
            help='JDBC input URL'
        )
        group.add_argument(
            f'--{constants.JDBCTOGCS_INPUT_URL_SECRET}',
            dest=constants.JDBCTOGCS_INPUT_URL_SECRET,
            required=False,
            default="",
            help='JDBC input URL secret name'
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
            required=False,
            help='JDBC input table name'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_INPUT_SQL_QUERY}',
            dest=constants.JDBCTOGCS_INPUT_SQL_QUERY,
            required=False,
            help='JDBC input SQL query'
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
            f'--{constants.JDBCTOGCS_INPUT_FETCHSIZE}',
            dest=constants.JDBCTOGCS_INPUT_FETCHSIZE,
            required=False,
            default=0,
            type=int,
            help='Determines how many rows to fetch per round trip'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_SESSIONINITSTATEMENT}',
            dest=constants.JDBCTOGCS_SESSIONINITSTATEMENT,
            required=False,
            default="",
            help='Custom SQL statement to execute in each reader database session'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_OUTPUT_LOCATION}',
            dest=constants.JDBCTOGCS_OUTPUT_LOCATION,
            required=True,
            help='Cloud Storage location for output files'
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
            help='Cloud Storage partition column name'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_TEMP_VIEW_NAME}',
            dest=constants.JDBCTOGCS_TEMP_VIEW_NAME,
            required=False,
            default="",
            help='Temp view name for creating a spark sql view on source data. This name has to match with the table name that will be used in the SQL query'
        )
        parser.add_argument(
            f'--{constants.JDBCTOGCS_TEMP_SQL_QUERY}',
            dest=constants.JDBCTOGCS_TEMP_SQL_QUERY,
            required=False,
            default="",
            help='SQL query for data transformation. This must use the temp view name as the table to query from.'
        )
        add_spark_options(parser, constants.get_csv_output_spark_options("jdbc.gcs.output."))

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        if getattr(known_args, constants.JDBCTOGCS_INPUT_TABLE) and getattr(known_args, constants.JDBCTOGCS_INPUT_SQL_QUERY):
            sys.exit('ArgumentParser Error: Arguments cannot have both input table and sql query, use either one.')

        if getattr(known_args, constants.JDBCTOGCS_TEMP_SQL_QUERY) and not getattr(known_args, constants.JDBCTOGCS_TEMP_VIEW_NAME):
            sys.exit('ArgumentParser Error: Temp view name cannot be null if you want to do data transformations with query')

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        #check if secret is passed or the connection string in URL
        #check if secret is passed or the connection string in the agruments
        if str(args[constants.JDBCTOGCS_INPUT_URL])=="":
            input_jdbc_url: str = secret_manager.access_secret_version(args[constants.JDBCTOGCS_INPUT_URL_SECRET])
        else:
            input_jdbc_url: str = args[constants.JDBCTOGCS_INPUT_URL]
        

        input_jdbc_driver: str = args[constants.JDBCTOGCS_INPUT_DRIVER]
        input_jdbc_table: str = args[constants.JDBCTOGCS_INPUT_TABLE]
        input_jdbc_sql_query: str = args[constants.JDBCTOGCS_INPUT_SQL_QUERY]
        input_jdbc_partitioncolumn: str = args[constants.JDBCTOGCS_INPUT_PARTITIONCOLUMN]
        input_jdbc_lowerbound: str = args[constants.JDBCTOGCS_INPUT_LOWERBOUND]
        input_jdbc_upperbound: str = args[constants.JDBCTOGCS_INPUT_UPPERBOUND]
        jdbc_numpartitions: str = args[constants.JDBCTOGCS_NUMPARTITIONS]
        input_jdbc_fetchsize: int = args[constants.JDBCTOGCS_INPUT_FETCHSIZE]
        input_jdbc_sessioninitstatement: str = args[constants.JDBCTOGCS_SESSIONINITSTATEMENT]
        output_location: str = args[constants.JDBCTOGCS_OUTPUT_LOCATION]
        output_format: str = args[constants.JDBCTOGCS_OUTPUT_FORMAT]
        output_mode: str = args[constants.JDBCTOGCS_OUTPUT_MODE]
        output_partitioncolumn: str = args[constants.JDBCTOGCS_OUTPUT_PARTITIONCOLUMN]
        temp_view: str = args[constants.JDBCTOGCS_TEMP_VIEW_NAME]
        temp_sql_query:str = args[constants.JDBCTOGCS_TEMP_SQL_QUERY]

        ignore_keys = {constants.JDBCTOGCS_INPUT_URL}
        filtered_args = {key:val for key,val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting JDBC to Cloud Storage Spark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
        )

        # Read
        input_data: DataFrame

        read_properties = {constants.JDBC_URL: input_jdbc_url,
                           constants.JDBC_DRIVER: input_jdbc_driver}

        if input_jdbc_table:
            read_properties.update({constants.JDBC_TABLE: input_jdbc_table})
        elif input_jdbc_sql_query:
            read_properties.update({constants.JDBC_QUERY: input_jdbc_sql_query})
        else:
            logger.error("Arguments must have either input table or input SQL query")
            exit(1)

        read_properties.update({constants.JDBC_NUMPARTITIONS: jdbc_numpartitions,
                                constants.JDBC_FETCHSIZE: input_jdbc_fetchsize})

        if input_jdbc_sessioninitstatement:
            read_properties[constants.JDBC_SESSIONINITSTATEMENT] = input_jdbc_sessioninitstatement

        partition_parameters = str(input_jdbc_partitioncolumn) + str(input_jdbc_lowerbound) + str(input_jdbc_upperbound)
        if ((partition_parameters != "") & ((input_jdbc_partitioncolumn == "") | (input_jdbc_lowerbound == "") | (input_jdbc_upperbound == ""))):
            logger.error("Set all the sql partitioning parameters together-jdbctogcs.input.partitioncolumn,jdbctogcs.input.lowerbound,jdbctogcs.input.upperbound. Refer to README.md for more instructions.")
            exit(1)

        if partition_parameters:
            read_properties.update({constants.JDBC_PARTITIONCOLUMN: input_jdbc_partitioncolumn,
                                    constants.JDBC_LOWERBOUND: input_jdbc_lowerbound,
                                    constants.JDBC_UPPERBOUND: input_jdbc_upperbound})

        input_data = spark.read \
            .format(constants.FORMAT_JDBC) \
            .options(**read_properties) \
            .load()

        if temp_sql_query:
            # Create temp view on source data
            input_data.createGlobalTempView(temp_view)
            # Execute SQL
            output_data = spark.sql(temp_sql_query)
        else:
            output_data = input_data

        # Write
        if (output_partitioncolumn != ""):
            writer: DataFrameWriter = output_data.write.mode(output_mode).partitionBy(output_partitioncolumn)
        else:
            writer: DataFrameWriter = output_data.write.mode(output_mode)

        persist_dataframe_to_cloud_storage(writer, args, output_location, output_format, "jdbc.gcs.output.")