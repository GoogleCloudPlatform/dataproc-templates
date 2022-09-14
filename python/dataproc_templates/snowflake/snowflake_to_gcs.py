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
import dataproc_templates.util.template_constants as constants

__all__ = ['SnowflakeToGCSTemplate']


class SnowflakeToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from Snowflake to GCS
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_SF_URL}',
            dest=constants.SNOWFLAKE_TO_GCS_SF_URL,
            required=True,
            help='Snowflake connection URL'
        )

        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_SF_USER}',
            dest=constants.SNOWFLAKE_TO_GCS_SF_USER,
            required=True,
            help='Snowflake user name'
        )
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_SF_PASSWORD}',
            dest=constants.SNOWFLAKE_TO_GCS_SF_PASSWORD,
            required=True,
            help='Snowflake password'
        )
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_SF_DATABASE}',
            dest=constants.SNOWFLAKE_TO_GCS_SF_DATABASE,
            required=True,
            help='Snowflake database name'
        )
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_SF_WAREHOUSE}',
            dest=constants.SNOWFLAKE_TO_GCS_SF_WAREHOUSE,
            required=False,
            default="",
            help='Snowflake datawarehouse name'
        )
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_SF_AUTOPUSHDOWN}',
            dest=constants.SNOWFLAKE_TO_GCS_SF_AUTOPUSHDOWN,
            required=False,
            default="on",
            help='Snowflake Autopushdown (on|off)'
        )
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_SF_SCHEMA}',
            dest=constants.SNOWFLAKE_TO_GCS_SF_SCHEMA,
            required=False,
            default="",
            help='Snowflake Schema, the source table belongs to'
        )
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_SF_TABLE}',
            dest=constants.SNOWFLAKE_TO_GCS_SF_TABLE,
            required=False,
            default="",
            help='Snowflake table name'
        )
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_SF_QUERY}',
            dest=constants.SNOWFLAKE_TO_GCS_SF_QUERY,
            required=False,
            default="",
            help='Query to be executed on Snowflake to fetch \
                the desired dataset for migration'
        )
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_OUTPUT_LOCATION}',
            dest=constants.SNOWFLAKE_TO_GCS_OUTPUT_LOCATION,
            required=True,
            help='GCS output location where the migrated data will be placed'
        )
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_OUTPUT_MODE}',
            dest=constants.SNOWFLAKE_TO_GCS_OUTPUT_MODE,
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
            f'--{constants.SNOWFLAKE_TO_GCS_OUTPUT_FORMAT}',
            dest=constants.SNOWFLAKE_TO_GCS_OUTPUT_FORMAT,
            required=False,
            default=constants.FORMAT_CSV,
            help=(
                'Output write format '
                '(one of: avro,parquet,csv,json)'
                '(Defaults to csv)'
            ),
            choices=[
                constants.FORMAT_AVRO,
                constants.FORMAT_PRQT,
                constants.FORMAT_CSV,
                constants.FORMAT_JSON
            ]
        )
        
        parser.add_argument(
            f'--{constants.SNOWFLAKE_TO_GCS_PARTITION_COLUMN}',
            dest=constants.SNOWFLAKE_TO_GCS_PARTITION_COLUMN,
            required=False,
            default="",
            help='Column name to partition data by, in GCS bucket'
        )
        
        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)
        
        if (not getattr(known_args, constants.SNOWFLAKE_TO_GCS_SF_TABLE) 
            and not getattr(known_args, constants.SNOWFLAKE_TO_GCS_SF_QUERY)):

            sys.exit("ArgumentParser Error: Either of snowflake.to.gcs.sf.table OR snowflake.to.gcs.sf.query needs \
                to be provided as argument to read data from Snowflake")
            
        elif (getattr(known_args, constants.SNOWFLAKE_TO_GCS_SF_TABLE) 
            and getattr(known_args, constants.SNOWFLAKE_TO_GCS_SF_QUERY)):
            
            sys.exit("ArgumentParser Error: Both snowflake.to.gcs.sf.table AND snowflake.to.gcs.sf.query \
                cannot be provided as arguments at the same time. Pick either of the two.")

        return vars(known_args)
    
    def get_read_options(self, logger: Logger, args: Dict[str, Any]) -> "tuple[DataFrame, DataFrame]":
        
        # Arguments
        sf_url: str = args[constants.SNOWFLAKE_TO_GCS_SF_URL]
        sf_user: str = args[constants.SNOWFLAKE_TO_GCS_SF_USER]
        sf_pwd: str = args[constants.SNOWFLAKE_TO_GCS_SF_PASSWORD]
        sf_database: str = args[constants.SNOWFLAKE_TO_GCS_SF_DATABASE]
        sf_schema: str = args[constants.SNOWFLAKE_TO_GCS_SF_SCHEMA]
        sf_warehouse: str = args[constants.SNOWFLAKE_TO_GCS_SF_WAREHOUSE]
        sf_autopushdown: str = args[constants.SNOWFLAKE_TO_GCS_SF_AUTOPUSHDOWN]
        sf_table: str = args[constants.SNOWFLAKE_TO_GCS_SF_TABLE]
        sf_query: str = args[constants.SNOWFLAKE_TO_GCS_SF_QUERY]
        
        logger.info(
            "Starting Snowflake to GCS spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )
        
        sf_options = { 
                     "sfURL" : sf_url,
                     "sfUser" : sf_user,
                     "sfPassword" : sf_pwd,
                     "sfDatabase" : sf_database,
                     "sfSchema" : sf_schema,
                     "sfWarehouse" : sf_warehouse,
                     "autopushdown" : sf_autopushdown
                    }
        
        data_options = {
            "dbtable" : sf_table,
            "query" : sf_query
        }
        
        return sf_options, data_options
       
    def read_data(self, logger: Logger, spark: SparkSession, sf_opt: Dict[str,Any], data_opt: Dict[str,Any] ) -> DataFrame:
        
        if not sf_opt or not data_opt:
            sys.exit("There seems to be an issue in fetching read options. Read options cannot be empty \n")
            
        # Read
        logger.info(
            "Starting process of reading data from source \n"
        )
        
        input_data: DataFrame = spark.read.format(constants.FORMAT_SNOWFLAKE) \
            .options(**sf_opt)
        
        if data_opt["dbtable"]:
            input_data = input_data.option("dbtable",data_opt["dbtable"])
        else:
            input_data = input_data.option("query",data_opt["query"])
            
        input_data = input_data.load()        
        count = input_data.count()
        
        if not count :
            sys.exit("The input dataframe is empty. The table is either empty or there is no data for the selected filters")
        else:
            logger.info(
            "Data from source has been read successfully \n"
            )
            
        return input_data
            
    def write_data(self, logger: Logger, args: Dict[str, Any], input_data: DataFrame) -> None:   
        
        gcs_output_format: str = args[constants.SNOWFLAKE_TO_GCS_OUTPUT_FORMAT]
        gcs_output_location: str = args[constants.SNOWFLAKE_TO_GCS_OUTPUT_LOCATION]
        gcs_output_mode: str = args[constants.SNOWFLAKE_TO_GCS_OUTPUT_MODE]
        gcs_partition_col: str = args[constants.SNOWFLAKE_TO_GCS_PARTITION_COLUMN]
                    
        # Write
        logger.info(
            "Starting process of writing data to GCS \n"
        )
        
        if gcs_partition_col:
            writer: DataFrameWriter = input_data.write.mode(gcs_output_mode) \
                .partitionBy(gcs_partition_col)
        else:
            writer: DataFrameWriter = input_data.write.mode(gcs_output_mode)

        if gcs_output_format == constants.FORMAT_PRQT:
            writer \
                .parquet(gcs_output_location)
        elif gcs_output_format == constants.FORMAT_AVRO:
            writer \
                .format(constants.FORMAT_AVRO) \
                .save(gcs_output_location)
        elif gcs_output_format == constants.FORMAT_CSV:
            writer \
                .option(constants.HEADER, True) \
                .csv(gcs_output_location)
        elif gcs_output_format == constants.FORMAT_JSON:
            writer \
                .json(gcs_output_location)
                
        logger.info(
            "Data from source has been loaded to GCS successfully"
        )


    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:  
        
        logger: Logger = self.get_logger(spark=spark)  
        
        sf_options, data_options = self.get_read_options(logger, args)
        
        input_data = self.read_data(logger, spark, sf_options, data_options)
        
        self.write_data(logger, args, input_data)
        
        
        
        
