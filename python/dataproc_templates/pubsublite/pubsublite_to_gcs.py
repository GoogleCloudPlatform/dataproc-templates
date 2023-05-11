from typing import Dict, Sequence, Optional, Any
from logging import Logger
import argparse
import pprint

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import to_json

from dataproc_templates import BaseTemplate
from dataproc_templates.util.argument_parsing import add_spark_options
import dataproc_templates.util.template_constants as constants
from dataproc_templates.util.dataframe_writer_wrappers import persist_streaming_dataframe_to_cloud_storage

__all__ = ['PubSubLiteToGCSTemplate']

class PubSubLiteToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Pub/Sub Lite to Cloud Storage
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL}',
            dest=constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL,
            required=True,
            help='Pub/Sub Lite Input subscription url'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_WRITE_MODE}',
            dest=constants.PUBSUBLITE_TO_GCS_WRITE_MODE,
            required=False,
            default=constants.OUTPUT_MODE_APPEND,
            help=(
                'Output write mode '
                '(one of: append, update, complete) '
                '(Defaults to append)'
            ),
            choices=[
                constants.OUTPUT_MODE_APPEND,
                constants.OUTPUT_MODE_UPDATE,
                constants.OUTPUT_MODE_COMPLETE
            ]
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_OUTPUT_LOCATION}',
            dest=constants.PUBSUBLITE_TO_GCS_OUTPUT_LOCATION,
            required=True,
            help='Cloud Storage output Bucket URL'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_CHECKPOINT_LOCATION}',
            dest=constants.PUBSUBLITE_TO_GCS_CHECKPOINT_LOCATION,
            required=True,
            help='Temporary folder for checkpoint location'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_OUTPUT_FORMAT}',
            dest=constants.PUBSUBLITE_TO_GCS_OUTPUT_FORMAT,
            required=False,
            default=constants.FORMAT_JSON,
            help=(
                'Output Format to Cloud Storage '
                '(one of: json, csv, avro, parquet) '
                '(Defaults to json)'
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
        add_spark_options(
            parser,
            constants.get_csv_output_spark_options("pubsublite.to.gcs.output."),
            read_options=False
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
        pubsublite_checkpoint_location: str = args[constants.PUBSUBLITE_TO_GCS_CHECKPOINT_LOCATION]
        output_format: str = args[constants.PUBSUBLITE_TO_GCS_OUTPUT_FORMAT]
        timeout: int = args[constants.PUBSUBLITE_TO_GCS_TIMEOUT]
        processing_time: str = args[constants.PUBSUBLITE_TO_GCS_PROCESSING_TIME]

        ignore_keys = {constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION_URL}
        filtered_args = {key:val for key,val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting Pub/Sub Lite to Cloud Storage spark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
        )

        # Read
        input_data=(spark.readStream \
            .format(constants.FORMAT_PUBSUBLITE) \
            .option(f"{constants.FORMAT_PUBSUBLITE}.subscription", input_subscription_url) \
            .load())
        
        input_data = input_data.withColumn("data", input_data.data.cast(StringType()))
        input_data = input_data.withColumn("attributes", to_json(input_data.attributes))

        # Write
        writer = input_data.writeStream \
            .trigger(processingTime=processing_time)

        writer = persist_streaming_dataframe_to_cloud_storage(
            writer, args, pubsublite_checkpoint_location, output_location,
            output_format, output_mode, "pubsublite.to.gcs.output.")

        query = writer.start()

        # Wait for some time (must be >= 60 seconds) to start receiving messages.
        query.awaitTermination(timeout)
        query.stop()
