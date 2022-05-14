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

__all__ = ['GCSToSpannerTemplate']


class GCSToSpannerTemplate(BaseTemplate):
  """
  Dataproc template implementing loads from GCS into Spanner
  """

  @staticmethod
  def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    parser: argparse.ArgumentParser = argparse.ArgumentParser()

    parser.add_argument(
      f'--{constants.GCS_SPANNER_INPUT_LOCATION}',
      dest=constants.GCS_SPANNER_INPUT_LOCATION,
      required=True,
      help='GCS location of the input files'
    )
    parser.add_argument(
      f'--{constants.GCS_SPANNER_INPUT_FORMAT}',
      dest=constants.GCS_SPANNER_INPUT_FORMAT,
      required=True,
      # TODO: Add support to CSV?
      help='Input file format (one of: <avro,parquet,orc>)',
      choices=[
        constants.FORMAT_AVRO,
        constants.FORMAT_PRQT,
        constants.FORMAT_ORC,
      ]
    )
    parser.add_argument(
      f'--{constants.GCS_SPANNER_OUTPUT_INSTANCE}',
      dest=constants.GCS_SPANNER_OUTPUT_INSTANCE,
      required=True,
      help='Spanner output instance'
    )
    parser.add_argument(
      f'--{constants.GCS_SPANNER_OUTPUT_DATABASE}',
      dest=constants.GCS_SPANNER_OUTPUT_DATABASE,
      required=True,
      help='Spanner output database'
    )
    parser.add_argument(
      f'--{constants.GCS_SPANNER_OUTPUT_TABLE}',
      dest=constants.GCS_SPANNER_OUTPUT_TABLE,
      required=True,
      help='Spanner output table'
    )
    parser.add_argument(
      f'--{constants.GCS_SPANNER_PRIMARY_KEY}',
      dest=constants.GCS_SPANNER_PRIMARY_KEY,
      required=True,
      help='Primary key of the table'
    )
    parser.add_argument(
      f'--{constants.GCS_SPANNER_OUTPUT_SAVE_MODE}',
      dest=constants.GCS_SPANNER_OUTPUT_SAVE_MODE,
      required=False,
      default=constants.OUTPUT_MODE_APPEND,
      help=(
        'Output write mode '
        '(one of: <Overwrite|ErrorIfExists|Append|Ignore>) '
        '(Defaults to ErrorIfExists)'
      ),
      choices=[
        constants.OUTPUT_MODE_OVERWRITE,
        constants.OUTPUT_MODE_APPEND,
        constants.OUTPUT_MODE_IGNORE,
        constants.OUTPUT_MODE_ERRORIFEXISTS
      ]
    )
    parser.add_argument(
      f'--{constants.GCS_SPANNER_OUTPUT_BATCH_INSERT_SIZE}',
      dest=constants.GCS_SPANNER_OUTPUT_BATCH_INSERT_SIZE,
      required=False,
      default=1000,
      help='Spanner batch insert size '
    )

    known_args: argparse.Namespace
    known_args, _ = parser.parse_known_args(args)

    return vars(known_args)

  def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

    project_id: str = constants.PROJECT_ID_PROP
    logger: Logger = self.get_logger(spark=spark)

    # Arguments
    input_file_location: str = args[constants.GCS_SPANNER_INPUT_LOCATION]
    input_file_format: str = args[constants.GCS_SPANNER_INPUT_FORMAT]
    spanner_instance: str = args[constants.GCS_SPANNER_OUTPUT_INSTANCE]
    spanner_database: str = args[constants.GCS_SPANNER_OUTPUT_DATABASE]
    spanner_table: str = args[constants.GCS_SPANNER_OUTPUT_TABLE]
    spanner_pk: str = args[constants.GCS_SPANNER_PRIMARY_KEY]
    spanner_save_mode: str = args[constants.GCS_SPANNER_OUTPUT_SAVE_MODE]
    batch_size: str = args[constants.GCS_SPANNER_OUTPUT_BATCH_INSERT_SIZE]
    spanner_url: str = (
      f"jdbc:cloudspanner:/projects/{project_id}/"
      f"instances/{spanner_instance}/"
      f"databases/{spanner_database}"
    )
    spanner_driver = "com.google.cloud.spanner.jdbc.JdbcDriver"
    table_options = f"PRIMARY KEY ({spanner_pk})"

    logger.info(
      "Starting GCS to Spanner spark job with parameters:\n"
      f"{pprint.pformat(args)}"
    )

    # Read
    input_data: DataFrame

    input_data = (
      spark.read
      .format(input_file_format)
      .load(input_file_location)
    )

    # Write
    (
      input_data.write
      .format("jdbc")
      .option("url", spanner_url)
      .option("dbtable", spanner_table)
      .option("createTableOptions", table_options)
      .option("isolationLevel", None)
      .option("batchsize", batch_size)
      .option("driver", spanner_driver)
      .mode(spanner_save_mode)
      .save()
    )
