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
from dataproc_templates.util.argument_parsing import add_spark_options, add_es_spark_connector_options
from dataproc_templates.util.dataframe_writer_wrappers import persist_dataframe_to_cloud_storage
from dataproc_templates.util.dataframe_reader_wrappers import ingest_dataframe_from_elasticsearch
from dataproc_templates.util.elasticsearch_transformations import flatten_struct_fields, flatten_array_fields
import dataproc_templates.util.template_constants as constants


__all__ = ['ElasticsearchToGCSTemplate']

class ElasticsearchToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Elasticsearch to Cloud Storage
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.ES_GCS_INPUT_NODE}',
            dest=constants.ES_GCS_INPUT_NODE,
            required=True,
            help='Elasticsearch Node Uri'
        )
        parser.add_argument(
            f'--{constants.ES_GCS_INPUT_INDEX}',
            dest=constants.ES_GCS_INPUT_INDEX,
            required=True,
            help='Elasticsearch Index Name'
        )
        parser.add_argument(
            f'--{constants.ES_GCS_NODE_USER}',
            dest=constants.ES_GCS_NODE_USER,
            required=True,
            help='Elasticsearch Node User'
        )
        parser.add_argument(
            f'--{constants.ES_GCS_NODE_PASSWORD}',
            dest=constants.ES_GCS_NODE_PASSWORD,
            required=True,
            help='Elasticsearch Node Password'
        )

        add_es_spark_connector_options(parser, constants.get_es_spark_connector_input_options("es.gcs.input."))

        parser.add_argument(
            f'--{constants.ES_GCS_FLATTEN_STRUCT}',
            dest=constants.ES_GCS_FLATTEN_STRUCT,
            action='store_true',
            required=False,
            help='Flatten the struct fields'
        )
        parser.add_argument(
            f'--{constants.ES_GCS_FLATTEN_ARRAY}',
            dest=constants.ES_GCS_FLATTEN_ARRAY,
            action='store_true',
            required=False,
            help=(
                'Flatten the n-D array fields to 1-D array fields,'
                f' it needs {constants.ES_GCS_FLATTEN_STRUCT} argument to be passed'
            )
        )
        parser.add_argument(
            f'--{constants.ES_GCS_OUTPUT_FORMAT}',
            dest=constants.ES_GCS_OUTPUT_FORMAT,
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
            f'--{constants.ES_GCS_OUTPUT_LOCATION}',
            dest=constants.ES_GCS_OUTPUT_LOCATION,
            required=True,
            help='Cloud Storage location for output files'
        )
        parser.add_argument(
            f'--{constants.ES_GCS_OUTPUT_MODE}',
            dest=constants.ES_GCS_OUTPUT_MODE,
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

        add_spark_options(parser, constants.get_csv_output_spark_options("es.gcs.output."))

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        es_node: str = args[constants.ES_GCS_INPUT_NODE]
        es_index: str = args[constants.ES_GCS_INPUT_INDEX]
        es_user: str = args[constants.ES_GCS_NODE_USER]
        es_password: str = args[constants.ES_GCS_NODE_PASSWORD]
        flatten_struct = args[constants.ES_GCS_FLATTEN_STRUCT]
        flatten_array = args[constants.ES_GCS_FLATTEN_ARRAY]
        output_format: str = args[constants.ES_GCS_OUTPUT_FORMAT]
        output_mode: str = args[constants.ES_GCS_OUTPUT_MODE]
        output_location: str = args[constants.ES_GCS_OUTPUT_LOCATION]

        ignore_keys = {constants.ES_GCS_NODE_PASSWORD}
        filtered_args = {key:val for key,val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting ElasticSearch to Cloud Storage Spark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
        )

        # Read
        input_data = ingest_dataframe_from_elasticsearch(
            spark, es_node, es_index, es_user, es_password, args, "es.gcs.input."
        )

        if flatten_struct:
            # Flatten the Struct Fields
            input_data = flatten_struct_fields(input_data)

            if flatten_array:
                # Flatten the n-D array fields to 1-D array fields
                input_data = flatten_array_fields(input_data)

        # Write
        writer: DataFrameWriter = input_data.write.mode(output_mode)
        persist_dataframe_to_cloud_storage(writer, args, output_location, output_format, "es.gcs.output.")
