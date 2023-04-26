from typing import Dict, Sequence, Optional, Any
from logging import Logger
import argparse
import pprint




from pyspark.sql import SparkSession


from dataproc_templates import BaseTemplate
from dataproc_templates.util.argument_parsing import add_spark_options
from dataproc_templates.util.dataframe_writer_wrappers import persist_streaming_dataframe_to_cloud_storage
import dataproc_templates.util.template_constants as constants

__all__ = ['KafkaToGCSTemplate']

class KafkaToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from Kafka into GCS
    """
    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.KAFKA_GCS_CHECKPOINT_LOCATION}',
            dest=constants.KAFKA_GCS_CHECKPOINT_LOCATION,
            required=True,
            help='Checkpoint location for Kafka to GCS Template'
        )
        parser.add_argument(
            f'--{constants.KAFKA_GCS_OUTPUT_LOCATION}',
            dest=constants.KAFKA_GCS_OUTPUT_LOCATION,
            required=True,
            help='GCS location of the destination folder'
        )
        parser.add_argument(
            f'--{constants.KAFKA_GCS_BOOTSTRAP_SERVERS}',
            dest=constants.KAFKA_GCS_BOOTSTRAP_SERVERS,
            required=True,
            help='Kafka topic address from where data is coming'
        )
        parser.add_argument(
            f'--{constants.KAFKA_TOPIC}',
            dest=constants.KAFKA_TOPIC,
            required=True,
            help='Kafka Topic Name'
        )
        parser.add_argument(
            f'--{constants.KAFKA_STARTING_OFFSET}',
            dest=constants.KAFKA_STARTING_OFFSET,
            required=True,
            help='Starting offset value (earliest, latest, json_string)'
        )
        parser.add_argument(
            f'--{constants.KAFKA_GCS_OUTPUT_FORMAT}',
            dest=constants.KAFKA_GCS_OUTPUT_FORMAT,
            required=True,
            help='Ouput format of the data (json , csv, avro, parquet)'
        )
        parser.add_argument(
            f'--{constants.KAFKA_GCS_OUPUT_MODE}',
            dest=constants.KAFKA_GCS_OUPUT_MODE,
            required=True,
            help="Ouput write mode (append, update, complete)",
            choices=[
                constants.OUTPUT_MODE_APPEND,
                constants.OUTPUT_MODE_UPDATE,
                constants.OUTPUT_MODE_COMPLETE
            ]
        )
        parser.add_argument(
            f'--{constants.KAFKA_GCS_TERMINATION_TIMEOUT}',
            dest=constants.KAFKA_GCS_TERMINATION_TIMEOUT,
            required=True,
            help="Timeout for termination of kafka subscription"
        )
        add_spark_options(
            parser,
            constants.get_csv_output_spark_options("kafka.gcs.output."),
            read_options=False
            )



        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        #arguments
        bootstrap_server_list: str = args[constants.KAFKA_GCS_BOOTSTRAP_SERVERS]
        gcs_output_location: str = args[constants.KAFKA_GCS_OUTPUT_LOCATION]
        kafka_topics: str= args[constants.KAFKA_TOPIC]
        output_format: str= args[constants.KAFKA_GCS_OUTPUT_FORMAT]
        output_mode:str = args[constants.KAFKA_GCS_OUPUT_MODE]
        timeout: int = int(args[constants.KAFKA_GCS_TERMINATION_TIMEOUT])
        offset:str = args[constants.KAFKA_STARTING_OFFSET]
        checkpoint_loc: str = args[constants.KAFKA_GCS_CHECKPOINT_LOCATION]

        ignore_keys = {constants.KAFKA_GCS_BOOTSTRAP_SERVERS}
        filtered_args = {key:val for key,val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting Kafka to GCS Pyspark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
        )


        df = spark.readStream.format(constants.KAFKA_INPUT_FORMAT) \
                  .option('kafka.bootstrap.servers', bootstrap_server_list) \
                  .option('subscribe', kafka_topics) \
                  .option('startingOffsets',offset) \
                  .load()

        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        # Write
        writer = df.writeStream

        writer = persist_streaming_dataframe_to_cloud_storage(
            writer, args, checkpoint_loc, gcs_output_location,
            output_format, output_mode, "kafka.gcs.output.")

        query = writer.start()

        query.awaitTermination(timeout)
        query.stop()
