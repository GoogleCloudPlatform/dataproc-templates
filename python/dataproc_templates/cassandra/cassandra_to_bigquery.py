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

from pyspark.sql import SparkSession, DataFrameWriter

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['CassandraToBQTemplate']


class CassandraToBQTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Cassandra to BQ
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.CASSANDRA_TO_BQ_INPUT_TABLE}',
            dest=constants.CASSANDRA_TO_BQ_INPUT_TABLE,
            required=True,
            help='Cassandra to BQ Input table name'
        )
        parser.add_argument(
            f'--{constants.CASSANDRA_TO_BQ_INPUT_HOST}',
            dest=constants.CASSANDRA_TO_BQ_INPUT_HOST,
            required=True,
            help='Input hostname for Cassandra cluster'
        )
        parser.add_argument(
            f'--{constants.CASSANDRA_TO_BQ_BIGQUERY_LOCATION}',
            dest=constants.CASSANDRA_TO_BQ_BIGQUERY_LOCATION,
            required=True,
            help='Target table in BQ Format: <dataset>.<table-name>'
        )
        parser.add_argument(
            f'--{constants.CASSANDRA_TO_BQ_WRITE_MODE}',
            dest=constants.CASSANDRA_TO_BQ_WRITE_MODE,
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
            f'--{constants.CASSANDRA_TO_BQ_TEMP_LOCATION}',
            dest=constants.CASSANDRA_TO_BQ_TEMP_LOCATION,
            required=True,
            help='GCS location for staging, Format: <bucket-name>'
        )
        parser.add_argument(
            f'--{constants.CASSANDRA_TO_BQ_QUERY}',
            dest=constants.CASSANDRA_TO_BQ_QUERY,
            required=False,
            help='Optional query for selective exports'
        )
        parser.add_argument(
            f'--{constants.CASSANDRA_TO_BQ_CATALOG}',
            dest=constants.CASSANDRA_TO_BQ_CATALOG,
            required=False,
            default="casscon",
            help='To provide a name for connection between Cassandra and BQ'
        )
        parser.add_argument(
            f'--{constants.CASSANDRA_TO_BQ_INPUT_KEYSPACE}',
            dest=constants.CASSANDRA_TO_BQ_INPUT_KEYSPACE,
            required=True,
            help='Keyspace Name of Cassandra Table'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_table: str = args[constants.CASSANDRA_TO_BQ_INPUT_TABLE]
        input_host: str = args[constants.CASSANDRA_TO_BQ_INPUT_HOST]
        output_mode: str = args[constants.CASSANDRA_TO_BQ_WRITE_MODE]
        output_location: str = args[constants.CASSANDRA_TO_BQ_BIGQUERY_LOCATION]
        catalog: str = args[constants.CASSANDRA_TO_BQ_CATALOG]
        query: str = args[constants.CASSANDRA_TO_BQ_QUERY]
        tempLocation: str = args[constants.CASSANDRA_TO_BQ_TEMP_LOCATION]
        input_keyspace: str = args[constants.CASSANDRA_TO_BQ_INPUT_KEYSPACE]

        logger.info(
            "Starting Cassandra to Bigquery spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )
        # Set configuration to connect to Cassandra by overwriting the spark session
        spark = (
            SparkSession
                .builder
                .appName("CassandraToBQ")
                .config(constants.SQL_EXTENSION, constants.CASSANDRA_EXTENSION)
                .config(f"spark.sql.catalog.{catalog}", constants.CASSANDRA_CATALOG)
                .config(f"spark.sql.catalog.{catalog}.spark.cassandra.connection.host",input_host)
                .getOrCreate())
        # Read
        if(not query):
            input_data = spark.read.table(f"{catalog}.{input_keyspace}.{input_table}")
        else:
            input_data= spark.sql(query)
        # Write
        input_data.write.format(constants.FORMAT_BIGQUERY)\
            .mode(output_mode).option(constants.TABLE, output_location)\
            .option(constants.TEMP_GCS_BUCKET, tempLocation).save()