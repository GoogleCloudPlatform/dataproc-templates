import os
from typing import Dict, Sequence, Optional, Any
from logging import Logger
import argparse
import pprint

from pyspark.sql import SparkSession, DataFrame, DataFrameWriter
from pyspark.sql.types import StringType

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['PubsubliteToGCSTemplate']

class PubsubliteToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Pubsublite to GCS
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_OUTPUT_PROJECT_ID}',
            dest=constants.PUBSUBLITE_TO_GCS_OUTPUT_PROJECT_ID,
            required=True,
            help='GCS Project ID'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION}',
            dest=constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION,
            required=True,
            help='Pubsublite to GCS Input subscription name'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_WRITE_MODE}',
            dest=constants.PUBSUBLITE_TO_GCS_WRITE_MODE,
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
            f'--{constants.PUBSUBLITE_TO_GCS_OUTPUT_LOCATION}',
            dest=constants.PUBSUBLITE_TO_GCS_OUTPUT_LOCATION,
            required=True,
            help='GCS output Bucket URL'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_CHECKPOINT_LOCATION}',
            dest=constants.PUBSUBLITE_CHECKPOINT_LOCATION,
            required=True,
            help='Temporary folder for checkpoint location'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_OUTPUT_FORMAT}',
            dest=constants.PUBSUBLITE_TO_GCS_OUTPUT_FORMAT,
            required=False,
            default=constants.FORMAT_CSV
            help=(
                'Output Format to GCS '
                '(one of: json, csv, avro, parquet) '
                '(Defaults to csv)'
            ),
            choices=[
                constants.FORMAT_AVRO,
                constants.FORMAT_CSV,
                constants.FORMAT_JSON,
                constants.FORMAT_PRQT
            ]
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        output_project_id: str = args[constants.PUBSUBLITE_TO_GCS_OUTPUT_PROJECT_ID]
        input_subscription: str = args[constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION]
        output_location: str = args[constants.PUBSUBLITE_TO_GCS_OUTPUT_LOCATION]
        output_mode: str = args[constants.PUBSUBLITE_TO_GCS_WRITE_MODE]
        pubsublite_checkpoint_location: str = args[constants.PUBSUBLITE_CHECKPOINT_LOCATION]
        output_format: str = args[constants.PUBSUBLITE_TO_GCS_OUTPUT_FORMAT]

        logger.info(
            "Starting Pubsublite to GCS spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )
        # Set configuration to connect to Pubsublite by overwriting the spark session
        spark = (
            SparkSession
                .builder
                .appName("Pubsublite To GCS Dataproc Job")
                .master("yarn")
                .getOrCreate())

        # Read
        input_data=(spark.readStream \
            .format(constants.FORMAT_PUBSUBLITE) \
            .option(f"{constants.FORMAT_PUBSUBLITE}.subscription", input_subscription,) \
            .load())

        # Write
        query = (input_data.writeStream \
            .format(output_format) \
            .option("checkpointLocation", pubsublite_checkpoint_location) \
            .option("path", output_location) \
            .outputMode(output_mode) \
            .trigger(processingTime="1 second") \
            .start())

        # Wait 120 seconds (must be >= 60 seconds) to start receiving messages.
        query.awaitTermination(120)
        query.stop()