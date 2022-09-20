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
from pyspark.sql import SparkSession

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['HiveToBigQueryTemplate']


class HiveToBigQueryTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Hive to BigQuery
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.HIVE_BQ_INPUT_DATABASE}',
            dest=constants.HIVE_BQ_INPUT_DATABASE,
            required=True,
            help='Hive database for importing data to BigQuery'
        )

        parser.add_argument(
            f'--{constants.HIVE_BQ_INPUT_TABLE}',
            dest=constants.HIVE_BQ_INPUT_TABLE,
            required=True,
            help='Hive table for importing data to BigQuery'
        )
        parser.add_argument(
            f'--{constants.HIVE_BQ_OUTPUT_DATASET}',
            dest=constants.HIVE_BQ_OUTPUT_DATASET,
            required=True,
            help='BigQuery dataset for the output table'
        )

        parser.add_argument(
            f'--{constants.HIVE_BQ_OUTPUT_TABLE}',
            dest=constants.HIVE_BQ_OUTPUT_TABLE,
            required=True,
            help='BigQuery output table name'
        )

        parser.add_argument(
            f'--{constants.HIVE_BQ_LD_TEMP_BUCKET_NAME}',
            dest=constants.HIVE_BQ_LD_TEMP_BUCKET_NAME,
            required=True,
            help='Spark BigQuery connector temporary bucket'
        )

        parser.add_argument(
            f'--{constants.HIVE_BQ_OUTPUT_MODE}',
            dest=constants.HIVE_BQ_OUTPUT_MODE,
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
            f'--{constants.HIVE_BQ_TEMP_VIEW_NAME}',
            dest=constants.HIVE_BQ_TEMP_VIEW_NAME,
            required=False,
            default="",
            help='Temp view name for creating a spark sql view on source data. This name has to match with the table name that will be used in the SQL query'
        )
        parser.add_argument(
            f'--{constants.HIVE_BQ_SQL_QUERY}',
            dest=constants.HIVE_BQ_SQL_QUERY,
            required=False,
            default="",
            help='SQL query for data transformation. This must use the temp view name as the table to query from.'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        if getattr(known_args, constants.HIVE_BQ_SQL_QUERY) and not getattr(known_args, constants.HIVE_BQ_TEMP_VIEW_NAME):
            sys.exit('ArgumentParser Error: Temp view name cannot be null if you want to do data transformations with query')

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        hive_database: str = args[constants.HIVE_BQ_INPUT_DATABASE]
        hive_table: str = args[constants.HIVE_BQ_INPUT_TABLE]
        bigquery_dataset: str = args[constants.HIVE_BQ_OUTPUT_DATASET]
        bigquery_table: str = args[constants.HIVE_BQ_OUTPUT_TABLE]
        bq_temp_bucket: str = args[constants.HIVE_BQ_LD_TEMP_BUCKET_NAME]
        output_mode: str = args[constants.HIVE_BQ_OUTPUT_MODE]
        temp_view: str = args[constants.HIVE_BQ_TEMP_VIEW_NAME]
        sql_query: str = args[constants.HIVE_BQ_SQL_QUERY]

        logger.info(
            "Starting Hive to Bigquery spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data = spark.table(hive_database + "." + hive_table)

        if sql_query:
            # Create temp view on source data
            input_data.createGlobalTempView(temp_view)
            # Execute SQL
            output_data = spark.sql(sql_query)
        else:
            output_data = input_data

        # Write
        output_data.write \
            .format(constants.FORMAT_BIGQUERY) \
            .option(constants.TABLE, bigquery_dataset + "." + bigquery_table) \
            .option(constants.TEMP_GCS_BUCKET, bq_temp_bucket) \
            .mode(output_mode) \
            .save()
