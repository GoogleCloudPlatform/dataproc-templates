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

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


__all__ = ['CassandraToGCSTemplate']


class CassandraToGCSTemplate(BaseTemplate):
   """
   Dataproc template implementing exports from CASSANDRA to GCS
   """

   @staticmethod
   def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
       parser: argparse.ArgumentParser = argparse.ArgumentParser()

       parser.add_argument(
           f'--{constants.CASSANDRA_TO_GCS_INPUT_HOST}',
           dest=constants.CASSANDRA_TO_GCS_INPUT_HOST,
           required=True,
           help='CASSANDRA Cloud Storage Input Host IP'
       )
       parser.add_argument(
           f'--{constants.CASSANDRA_TO_GCS_OUTPUT_FORMAT}',
           dest=constants.CASSANDRA_TO_GCS_OUTPUT_FORMAT,
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
           f'--{constants.CASSANDRA_TO_GCS_OUTPUT_PATH}',
           dest=constants.CASSANDRA_TO_GCS_OUTPUT_PATH,
           required=True,
           help='Cloud Storage location for output files'
       )
       parser.add_argument(
           f'--{constants.CASSANDRA_TO_GCS_OUTPUT_SAVEMODE}',
           dest=constants.CASSANDRA_TO_GCS_OUTPUT_SAVEMODE,
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
            f'--{constants.CASSANDRA_TO_GCS_CATALOG}',
            dest=constants.CASSANDRA_TO_GCS_CATALOG,
            required=False,
            default="casscon",
            help='To provide a name for connection between Cassandra and GCS'
       )
       parser.add_argument(
            f'--{constants.CASSANDRA_TO_GCS_QUERY}',
            dest=constants.CASSANDRA_TO_GCS_QUERY,
            required=False,
            help='Optional query for selective exports'
       )

       parser.add_argument(
            f'--{constants.CASSANDRA_TO_GCS_INPUT_KEYSPACE}',
            dest=constants.CASSANDRA_TO_GCS_INPUT_KEYSPACE,
            required=(constants.CASSANDRA_TO_GCS_QUERY is None),
            help='CASSANDRA Cloud Storage Input Keyspace'
       )
       parser.add_argument(
           f'--{constants.CASSANDRA_TO_GCS_INPUT_TABLE}',
           dest=constants.CASSANDRA_TO_GCS_INPUT_TABLE,
           required=(constants.CASSANDRA_TO_GCS_QUERY is None),
           help='CASSANDRA Cloud Storage Input Table'
       )
       add_spark_options(parser, constants.get_csv_output_spark_options("cassandra.gcs.output."))

       known_args: argparse.Namespace
       known_args, _ = parser.parse_known_args(args)
       if (not getattr(known_args, constants.CASSANDRA_TO_GCS_QUERY)
            and (not getattr(known_args, constants.CASSANDRA_TO_GCS_INPUT_KEYSPACE)
            or not getattr(known_args, constants.CASSANDRA_TO_GCS_INPUT_TABLE))):

            sys.exit("ArgumentParser Error: Either of cassandratogcs.input.keyspace and cassandratogcs.input.table "
                        + "OR cassandratogcs.input.query needs to be provided as argument to read data from Cassandra")

       elif (getattr(known_args, constants.CASSANDRA_TO_GCS_QUERY)
            and (getattr(known_args, constants.CASSANDRA_TO_GCS_INPUT_KEYSPACE)
            or getattr(known_args, constants.CASSANDRA_TO_GCS_INPUT_TABLE))):

            sys.exit("ArgumentParser Error: Both cassandratogcs.input.keyspace and cassandratogcs.input.table "
                        + "AND cassandratogcs.input.query cannot be provided as arguments at the same time.")


       return vars(known_args)

   def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

       logger: Logger = self.get_logger(spark=spark)

       # Arguments
       input_host: str = args[constants.CASSANDRA_TO_GCS_INPUT_HOST]
       input_keyspace: str = args[constants.CASSANDRA_TO_GCS_INPUT_KEYSPACE]
       input_table: str = args[constants.CASSANDRA_TO_GCS_INPUT_TABLE]
       output_format: str = args[constants.CASSANDRA_TO_GCS_OUTPUT_FORMAT]
       output_location: str = args[constants.CASSANDRA_TO_GCS_OUTPUT_PATH]
       output_mode: str = args[constants.CASSANDRA_TO_GCS_OUTPUT_SAVEMODE]
       catalog: str = args[constants.CASSANDRA_TO_GCS_CATALOG]
       query: str = args[constants.CASSANDRA_TO_GCS_QUERY]

       logger.info(
           "Starting CASSANDRA to Cloud Storage spark job with parameters:\n"
           f"{pprint.pformat(args)}"
       )

       # Set configuration to connect to Cassandra by overwriting the spark session
       spark = (
        SparkSession
        .builder
        .appName("CassandraToGCS")
        .config(constants.SQL_EXTENSION, constants.CASSANDRA_EXTENSION)
        .config(f"spark.sql.catalog.{catalog}", constants.CASSANDRA_CATALOG)
        .config(f"spark.sql.catalog.{catalog}.spark.cassandra.connection.host",input_host)
        .getOrCreate())

       # Read
       if(not query):
           input_data = spark.read.table(f"{catalog}.{input_keyspace}.{input_table}")
       else:
           input_data = spark.sql(query)

       # Write
       writer: DataFrameWriter = input_data.write.mode(output_mode)
       persist_dataframe_to_cloud_storage(writer, args, output_location, output_format, "cassandratogcs.output.")
