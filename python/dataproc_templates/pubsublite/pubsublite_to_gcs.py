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
            f'--{constants.PUBSUBLITE_TO_GCS_INPUT_PROJECT_ID}',
            dest=constants.PUBSUBLITE_TO_GCS_INPUT_PROJECT_ID,
            required=True,
            help='Pubsublite Project ID'
        )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_OUTPUT_PROJECT_ID}',
            dest=constants.PUBSUBLITE_TO_GCS_OUTPUT_PROJECT_ID,
            required=True,
            help='GCS Project ID'
        )
        # parser.add_argument(
        #     f'--{constants.PUBSUBLITE_TO_GCS_INPUT_TOPIC}',
        #     dest=constants.PUBSUBLITE_TO_GCS_INPUT_TOPIC,
        #     required=False,
        #     help='Pubsublite to GCS Input topic name'
        # )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION}',
            dest=constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION,
            required=True,
            help='Pubsublite to GCS Input subscription name'
        )
        # parser.add_argument(
        #     f'--{constants.PUBSUBLITE_TO_GCS_INPUT_TIMEOUT_MS}',
        #     dest=constants.PUBSUBLITE_TO_GCS_INPUT_TIMEOUT_MS,
        #     required=False,
        #     default=60000,
        #     help='Stream timeout, for how long the subscription will be read'
        # )
        # parser.add_argument(
        #     f'--{constants.PUBSUBLITE_TO_GCS_STREAMING_DURATION_SECONDS}',
        #     dest=constants.PUBSUBLITE_TO_GCS_STREAMING_DURATION_SECONDS,
        #     required=False,
        #     default=15,
        #     help='Streaming duration, how often wil writes to GCS be triggered'
        # )
        # parser.add_argument(
        #     f'--{constants.PUBSUBLITE_TO_GCS_WRITE_MODE}',
        #     dest=constants.PUBSUBLITE_TO_GCS_WRITE_MODE,
        #     required=False,
        #     default=constants.OUTPUT_MODE_APPEND,
        #     help=(
        #         'Output write mode '
        #         '(one of: append,overwrite,ignore,errorifexists) '
        #         '(Defaults to append)'
        #     ),
        #     choices=[
        #         constants.OUTPUT_MODE_OVERWRITE,
        #         constants.OUTPUT_MODE_APPEND,
        #         constants.OUTPUT_MODE_IGNORE,
        #         constants.OUTPUT_MODE_ERRORIFEXISTS
        #     ]
        # )
        # parser.add_argument(
        #     f'--{constants.PUBSUBLITE_TO_GCS_TOTAL_RECEIVERS}',
        #     dest=constants.PUBSUBLITE_TO_GCS_TOTAL_RECEIVERS,
        #     required=False,
        #     default=5,
        #     help='PUBSUBLITE_TO_GCS_TOTAL_RECEIVERS'
        # )
        parser.add_argument(
            f'--{constants.PUBSUBLITE_TO_GCS_OUTPUT_LOCATION}',
            dest=constants.PUBSUBLITE_TO_GCS_OUTPUT_LOCATION,
            required=True,
            help='GCS output Bucket URL'
        )
        # parser.add_argument(
        #     f'--{constants.PUBSUBLITE_TO_GCS_BATCH_SIZE}',
        #     dest=constants.PUBSUBLITE_TO_GCS_BATCH_SIZE,
        #     required=True,
        #     help='Number of records to be written per message to GCS'
        # )
        # parser.add_argument(
        #     f'--{constants.PUBSUBLITE_CHECKPOINT_LOCATION}',
        #     dest=constants.PUBSUBLITE_CHECKPOINT_LOCATION,
        #     required=True,
        #     help='Temporary folder for checkpoint location'
        # )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        input_project_id: str = args[constants.PUBSUBLITE_TO_GCS_INPUT_PROJECT_ID]
        output_project_id: str = args[constants.PUBSUBLITE_TO_GCS_OUTPUT_PROJECT_ID]
        input_subscription: str = args[constants.PUBSUBLITE_TO_GCS_INPUT_SUBSCRIPTION]
        # timeout_ms: int = args[constants.PUBSUBLITE_TO_GCS_INPUT_TIMEOUT_MS]
        # streaming_duration: int = args[constants.PUBSUBLITE_TO_GCS_STREAMING_DURATION_SECONDS]
        # total_receivers: int = args[constants.PUBSUBLITE_TO_GCS_TOTAL_RECEIVERS]
        output_location: str = args[constants.PUBSUBLITE_TO_GCS_OUTPUT_LOCATION]
        # batch_size: int = args[constants.PUBSUBLITE_TO_GCS_BATCH_SIZE]
        # output_mode: str = args[constants.PUBSUBLITE_TO_GCS_WRITE_MODE]
        # pubsublite_checkpoint_location: str = args[constants.PUBSUBLITE_CHECKPOINT_LOCATION]

        logger.info(
            "Starting Pubsublite to GCS spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )
        # Set configuration to connect to Pubsublite by overwriting the spark session
        spark = (
            SparkSession
                .builder
                .appName("read-app")
                .master("yarn")
                .getOrCreate())

        # Read
        input_data=(spark.readStream \
            .format(constants.FORMAT_PUBSUBLITE) \
            .option(f"{constants.FORMAT_PUBSUBLITE}.subscription",f"projects/{input_project_id}/locations/{os.getenv('REGION')}/subscriptions/{input_subscription}",) \
            .load())

        input_data.withColumn("data", input_data.data.cast(StringType()))
        print("test test test")
        print(input_data)
        #logger.info(input_data)

        # Write
        query = (input_data.writeStream \
            .format("csv") \
            .option("temporaryGcsBucket", "gs://saumyasinha/pubsublite/tempBucket") \
            .option("checkpointLocation", "gs://saumyasinha/pubsublite/checkpointLocation") \
            .option("path", output_location) \
            .trigger(processingTime="1 second") \
            .start())
        # query.awaitTermination(120)
        query.stop()
        # query = (input_data.writeStream \
        #     .format(constants.FORMAT_BIGQUERY) \
        #     .option("temporaryGcsBucket", bq_temp_bucket) \
        #     .option("checkpointLocation", pubsublite_checkpoint_location) \
        #     .option("table", f"{output_project_id}.{output_dataset}.{output_table}") \
        #     .trigger(processingTime="1 second") \
        #     .start())

        # Wait 120 seconds (must be >= 60 seconds) to start receiving messages.
        # query.awaitTermination(120)
        # query.stop()