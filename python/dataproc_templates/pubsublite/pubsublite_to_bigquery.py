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

from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.types import StringType

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['PubsubliteToBQTemplate']

class PubsubliteToBQTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Pubsublite to BQ
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_BQ_INPUT_TOPIC}',
            dest=constants.PUBSUBLITE_TO_BQ_INPUT_TOPIC,
            required=False,
            help='Pubsublite to BQ Input topic name'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_BQ_INPUT_SUBSCRIPTION}',
            dest=constants.PUBSUBLITE_TO_BQ_INPUT_SUBSCRIPTION,
            required=True,
            help='Pubsublite to BQ Input subscription name'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_BQ_INPUT_TIMEOUT_MS}',
            dest=constants.PUBSUBLITE_TO_BQ_INPUT_TIMEOUT_MS,
            required=False,
            default=60000,
            help='Stream timeout, for how long the subscription will be read'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_BQ_STREAMING_DURATION_SECONDS}',
            dest=constants.PUBSUBLITE_TO_BQ_STREAMING_DURATION_SECONDS,
            required=False,
            default=15,
            help='Streaming duration, how often wil writes to BQ be triggered'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_BQ_WRITE_MODE}',
            dest=constants.PUBSUBLITE_TO_BQ_WRITE_MODE,
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
            f'--{constants.PUBSUBLITE_TO_BQ_TOTAL_RECEIVERS}',
            dest=constants.PUBSUBLITE_TO_BQ_TOTAL_RECEIVERS,
            required=False,
            default=5,
            help='PUBSUBLITE_TO_BQ_TOTAL_RECEIVERS'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_BQ_PROJECT_ID}',
            dest=constants.PUBSUBLITE_TO_BQ_PROJECT_ID,
            required=True,
            help='BQ Project ID'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_BQ_OUTPUT_DATASET}',
            dest=constants.PUBSUBLITE_TO_BQ_OUTPUT_DATASET,
            required=True,
            help='BigQuery output dataset'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_BQ_OUTPUT_TABLE}',
            dest=constants.PUBSUBLITE_TO_BQ_OUTPUT_TABLE,
            required=True,
            help='BigQuery output table'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_BQ_BATCH_SIZE}',
            dest=constants.PUBSUBLITE_TO_BQ_BATCH_SIZE,
            required=False,
            default=1000,
            help='Number of records to be written per message to BigQuery'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_BQ_TEMPORARY_BUCKET}',
            dest=constants.PUBSUBLITE_TO_BQ_TEMPORARY_BUCKET,
            required=True,
            help='Temporary bucket for the Spark BigQuery connector'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_CHECKPOINT_LOCATION}',
            dest=constants.PUBSUBLITE_CHECKPOINT_LOCATION,
            required=True,
            help='Temporary folder for checkpoint location'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_subscription: str = args[constants.PUBSUBLITE_TO_BQ_INPUT_SUBSCRIPTION]
        output_project_id: str = args[constants.PUBSUBLITE_TO_BQ_PROJECT_ID]
        output_dataset: str = args[constants.PUBSUBLITE_TO_BQ_OUTPUT_DATASET]
        output_table: str = args[constants.PUBSUBLITE_TO_BQ_OUTPUT_TABLE]
        pubsublite_checkpoint_location: str = args[constants.PUBSUBLITE_CHECKPOINT_LOCATION]
        bq_temp_bucket: str = args[constants.PUBSUBLITE_TO_BQ_TEMPORARY_BUCKET]

        logger.info(
            "Starting Pubsublite to Bigquery spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )
        # Set configuration to connect to Cassandra by overwriting the spark session
        spark = (
            SparkSession
                .builder
                .appName("read-app")
                .master("yarn")
                .getOrCreate())

        # Read
        input_data=(spark.readStream \
            .format(constants.FORMAT_PUBSUBLITE) \
            .option(f"{constants.FORMAT_PUBSUBLITE}.subscription",f"projects/617357862702/locations/us-west1/subscriptions/{input_subscription}",) \
            .load())
        
        input_data.withColumn("data", input_data.data.cast(StringType()))

        # Write
        query = (input_data.writeStream \
            .format(constants.FORMAT_BIGQUERY) \
            .option("temporaryGcsBucket", bq_temp_bucket) \
            .option("checkpointLocation", pubsublite_checkpoint_location) \
            .option("table", f"{output_project_id}.{output_dataset}.{output_table}") \
            .trigger(processingTime="1 second") \
            .start())

        # Wait 120 seconds (must be >= 60 seconds) to start receiving messages.
        query.awaitTermination(120)
        query.stop()