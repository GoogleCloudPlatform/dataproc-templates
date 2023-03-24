from typing import Dict, Sequence, Optional, Any
from logging import Logger
import argparse
import pprint

from pyspark.sql import SparkSession

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['PubSubLiteToGCSTemplate']

class PubSubLiteToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from PubSubLite to GCS
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL}',
            dest=constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL,
            required=True,
            help='PubSubLite to GCS Input subscription url'
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
            default=constants.FORMAT_CSV,
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
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_TIMEOUT}',
            dest=constants.PUBSUBLITE_TO_GCS_TIMEOUT,
            required=True,
            type=int,
            help=('Time for which subscriptions will be read')
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_PROCESSING_TIME}',
            dest=constants.PUBSUBLITE_TO_GCS_PROCESSING_TIME,
            required=True,
            help=('Time interval at which the query will be triggered to process input data')
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_subscription_url: str = args[constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL]
        output_location: str = args[constants.PUBSUBLITE_TO_GCS_OUTPUT_LOCATION]
        output_mode: str = args[constants.PUBSUBLITE_TO_GCS_WRITE_MODE]
        pubsublite_checkpoint_location: str = args[constants.PUBSUBLITE_CHECKPOINT_LOCATION]
        output_format: str = args[constants.PUBSUBLITE_TO_GCS_OUTPUT_FORMAT]
        timeout: int = args[constants.PUBSUBLITE_TO_GCS_TIMEOUT]
        processing_time: str = args[constants.PUBSUBLITE_TO_GCS_PROCESSING_TIME]

        logger.info(
            "Starting PubSubLite to GCS spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data=(spark.readStream \
            .format(constants.FORMAT_PUBSUBLITE) \
            .option(f"{constants.FORMAT_PUBSUBLITE}.subscription", input_subscription_url) \
            .load())

        # Write
        query = (input_data.writeStream \
            .format(output_format) \
            .option("checkpointLocation", pubsublite_checkpoint_location) \
            .option("path", output_location) \
            .outputMode(output_mode) \
            .trigger(processingTime=processing_time) \
            .start())

        # Wait for some time (must be >= 60 seconds) to start receiving messages.
        query.awaitTermination(timeout)
        query.stop()