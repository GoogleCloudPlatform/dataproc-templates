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

from pyspark.sql import SparkSession

from dataproc_templates import BaseTemplate
from dataproc_templates.util.argument_parsing import add_es_spark_connector_options
from dataproc_templates.util.dataframe_reader_wrappers import ingest_dataframe_from_elasticsearch
from dataproc_templates.util.elasticsearch_transformations import flatten_struct_fields, flatten_array_fields
import dataproc_templates.util.template_constants as constants


__all__ = ['ElasticsearchToBigTableTemplate']

class ElasticsearchToBigTableTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Elasticsearch to BigTable
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.ES_BT_INPUT_NODE}',
            dest=constants.ES_BT_INPUT_NODE,
            required=True,
            help='Elasticsearch Node Uri'
        )
        parser.add_argument(
            f'--{constants.ES_BT_INPUT_INDEX}',
            dest=constants.ES_BT_INPUT_INDEX,
            required=True,
            help='Elasticsearch Index Name'
        )
        parser.add_argument(
            f'--{constants.ES_BT_NODE_USER}',
            dest=constants.ES_BT_NODE_USER,
            required=True,
            help='Elasticsearch Node User'
        )
        parser.add_argument(
            f'--{constants.ES_BT_NODE_PASSWORD}',
            dest=constants.ES_BT_NODE_PASSWORD,
            required=True,
            help='Elasticsearch Node Password'
        )

        add_es_spark_connector_options(parser, constants.get_es_spark_connector_input_options("es.bt.input."))

        parser.add_argument(
            f'--{constants.ES_BT_FLATTEN_STRUCT}',
            dest=constants.ES_BT_FLATTEN_STRUCT,
            action='store_true',
            required=False,
            help='Flatten the struct fields'
        )
        parser.add_argument(
            f'--{constants.ES_BT_FLATTEN_ARRAY}',
            dest=constants.ES_BT_FLATTEN_ARRAY,
            action='store_true',
            required=False,
            help=(
                'Flatten the n-D array fields to 1-D array fields,'
                f' it needs {constants.ES_BT_FLATTEN_STRUCT} to be true'
            )
        )
        parser.add_argument(
            f'--{constants.ES_BT_PROJECT_ID}',
            dest=constants.ES_BT_PROJECT_ID,
            required=True,
            help='BigTable project ID'
        )
        parser.add_argument(
            f'--{constants.ES_BT_INSTANCE_ID}',
            dest=constants.ES_BT_INSTANCE_ID,
            required=True,
            help='BigTable instance ID'
        )
        parser.add_argument(
            f'--{constants.ES_BT_CREATE_NEW_TABLE}',
            dest=constants.ES_BT_CREATE_NEW_TABLE,
            required=False,
            help='BigTable create new table flag. Default is false.',
            default=False
        )
        parser.add_argument(
            f'--{constants.ES_BT_BATCH_MUTATE_SIZE}',
            dest=constants.ES_BT_BATCH_MUTATE_SIZE,
            required=False,
            help='BigTable batch mutate size. Maximum allowed size is 100000. Default is 100.',
            default=100
        )
        parser.add_argument(
            f'--{constants.ES_BT_CATALOG_JSON}',
            dest=constants.ES_BT_CATALOG_JSON,
            required=True,
            help='BigTable catalog inline json'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        es_node: str = args[constants.ES_BT_INPUT_NODE]
        es_index: str = args[constants.ES_BT_INPUT_INDEX]
        es_user: str = args[constants.ES_BT_NODE_USER]
        es_password: str = args[constants.ES_BT_NODE_PASSWORD]
        flatten_struct = args[constants.ES_BT_FLATTEN_STRUCT]
        flatten_array = args[constants.ES_BT_FLATTEN_ARRAY]
        catalog: str = ''.join(args[constants.ES_BT_CATALOG_JSON].split())
        project_id: str = args[constants.ES_BT_PROJECT_ID]
        instance_id: str = args[constants.ES_BT_INSTANCE_ID]
        create_new_table: bool = args[constants.ES_BT_CREATE_NEW_TABLE]
        batch_mutate_size: int = args[constants.ES_BT_BATCH_MUTATE_SIZE]

        ignore_keys = {constants.ES_BT_NODE_PASSWORD}
        filtered_args = {key:val for key,val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting Elasticsearch to BigTable Spark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
        )

        # Read
        input_data = ingest_dataframe_from_elasticsearch(
            spark, es_node, es_index, es_user, es_password, args, "es.bt.input."
        )

        if flatten_struct:
            # Flatten the Struct Fields
            input_data = flatten_struct_fields(input_data)

            if flatten_array:
                # Flatten the n-D array fields to 1-D array fields
                input_data = flatten_array_fields(input_data)

        # Write
        input_data.write \
            .format(constants.FORMAT_BIGTABLE) \
            .options(catalog=catalog) \
            .option(constants.ES_BT_PROJECT_ID, project_id) \
            .option(constants.ES_BT_INSTANCE_ID, instance_id) \
            .option(constants.ES_BT_CREATE_NEW_TABLE, create_new_table) \
            .option(constants.ES_BT_BATCH_MUTATE_SIZE, batch_mutate_size) \
            .save()
