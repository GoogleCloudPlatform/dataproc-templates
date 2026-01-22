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

from pyspark.sql import SparkSession, DataFrameWriter

from dataproc_templates import BaseTemplate
from dataproc_templates.util.argument_parsing import add_es_spark_connector_options
from dataproc_templates.util.dataframe_writer_wrappers import persist_dataframe_to_elasticsearch
import dataproc_templates.util.template_constants as constants

__all__ = ['BigQueryToElasticsearchTemplate']

class BigQueryToElasticsearchTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from BigQuery to Elasticsearch
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.BQ_ES_INPUT_TABLE}',
            dest=constants.BQ_ES_INPUT_TABLE,
            required=True,
            help='BigQuery Input table name'
        )
        parser.add_argument(
            f'--{constants.BQ_ES_INPUT_TABLE_COLUMNS}',
            dest=constants.BQ_ES_INPUT_TABLE_COLUMNS,
            required=False,
            help='Comma Seperated list of columns to read from the input table'
        )
        parser.add_argument(
            f'--{constants.BQ_ES_INPUT_TABLE_FILTERS}',
            dest=constants.BQ_ES_INPUT_TABLE_FILTERS,
            required=False,
            help='Row level filters to apply when reading from the input table'
        )
        parser.add_argument(
            f'--{constants.BQ_ES_OUTPUT_NODE}',
            dest=constants.BQ_ES_OUTPUT_NODE,
            required=True,
            help='Elasticsearch Node Uri'
        )
        parser.add_argument(
            f'--{constants.BQ_ES_OUTPUT_INDEX}',
            dest=constants.BQ_ES_OUTPUT_INDEX,
            required=True,
            help='Elasticsearch Index Name'
        )
        parser.add_argument(
            f'--{constants.BQ_ES_OUTPUT_NODE_USER}',
            dest=constants.BQ_ES_OUTPUT_NODE_USER,
            help='Elasticsearch Node User'
        )
        parser.add_argument(
            f'--{constants.BQ_ES_OUTPUT_NODE_PASSWORD}',
            dest=constants.BQ_ES_OUTPUT_NODE_PASSWORD,
            help='Elasticsearch Node Password'
        )
        parser.add_argument(
            f'--{constants.BQ_ES_OUTPUT_NODE_API_KEY}',
            dest=constants.BQ_ES_OUTPUT_NODE_API_KEY,
            help='Elasticsearch Node API Key'
        )
        parser.add_argument(
            f'--{constants.BQ_ES_OUTPUT_MODE}',
            dest=constants.BQ_ES_OUTPUT_MODE,
            required=False,
            default=constants.OUTPUT_MODE_APPEND,
            help=(
                'Output write mode '
                '(one of: append,overwrite) '
                '(Defaults to append)'
            ),
            choices=[
                constants.OUTPUT_MODE_OVERWRITE,
                constants.OUTPUT_MODE_APPEND
            ]
        )
        add_es_spark_connector_options(parser, constants.get_es_spark_connector_writer_options("bigquery.elasticsearch.output."))

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        if (not getattr(known_args, constants.BQ_ES_OUTPUT_NODE_API_KEY)
            and (not getattr(known_args, constants.BQ_ES_OUTPUT_NODE_USER)
            or not getattr(known_args, constants.BQ_ES_OUTPUT_NODE_PASSWORD))):

            sys.exit("ArgumentParser Error: Either of bigquery.elasticsearch.output.user and bigquery.elasticsearch.otuput.password "
                        + "OR bigquery.elasticsearch.output.api.key needs to be provided as argument to read data from Elasticsearch")

        elif (getattr(known_args, constants.BQ_ES_OUTPUT_NODE_API_KEY)
            and (getattr(known_args, constants.BQ_ES_OUTPUT_NODE_USER)
            or getattr(known_args, constants.BQ_ES_OUTPUT_NODE_PASSWORD))):

            sys.exit("ArgumentParser Error: Both bigquery.elasticsearch.output.user and bigquery.elasticsearch.otuput.password "
                        + "AND bigquery.elasticsearch.output.api.key cannot be provided as arguments at the same time.")

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        bq_table: str = args[constants.BQ_ES_INPUT_TABLE]
        bq_table_columns: str = args[constants.BQ_ES_INPUT_TABLE_COLUMNS]
        bq_table_filters: str = args[constants.BQ_ES_INPUT_TABLE_FILTERS]
        es_node: str = args[constants.BQ_ES_OUTPUT_NODE]
        es_index: str = args[constants.BQ_ES_OUTPUT_INDEX]
        es_user: str = args[constants.BQ_ES_OUTPUT_NODE_USER]
        es_password: str = args[constants.BQ_ES_OUTPUT_NODE_PASSWORD]
        es_api_key: str = args[constants.BQ_ES_OUTPUT_NODE_API_KEY]
        output_mode: str = args[constants.BQ_ES_OUTPUT_MODE]

        logger.info(
            "Starting Bigquery to Elasticsearch Spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        bq_config = {
            "table": bq_table,
            "viewsEnabled": "true",
            "parentProject": bq_table.split(":")[0]
        }
        if bq_table_filters:
            bq_config["filter"] = bq_table_filters

        if bq_table_columns:
            bq_table_columns_list = [col.strip() for col in bq_table_columns.split(",")]

            input_data = spark.read \
                        .format("bigquery") \
                        .options(**bq_config) \
                        .load() \
                        .select(*bq_table_columns_list)
        else:
            input_data = spark.read \
                        .format("bigquery") \
                        .options(**bq_config) \
                        .load()

        # Write
        writer: DataFrameWriter = input_data.write.format(constants.FORMAT_ELASTICSEARCH_WRITER).mode(output_mode)
        persist_dataframe_to_elasticsearch(writer, args, es_node, es_index, es_user, es_password, es_api_key, "bigquery.elasticsearch.output.")

