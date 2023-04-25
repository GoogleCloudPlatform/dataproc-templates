from typing import Dict, Sequence, Optional, Any
from logging import Logger
import argparse
import pprint
import os


from pyspark.sql import SparkSession, DataFrame


from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['KafkaToBigQueryTemplate']

class KafkaToBigQueryTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from Kafka into Bigquery
    """
    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.KAFKA_BQ_CHECKPOINT_LOCATION}',
            dest=constants.KAFKA_BQ_CHECKPOINT_LOCATION,
            required=True,
            help='GCS location of the checkpoint folder'
        )
        parser.add_argument(
            f'--{constants.KAFKA_BOOTSTRAP_SERVERS}',
            dest=constants.KAFKA_BOOTSTRAP_SERVERS,
            required=True,
            help='Kafka topic address from where data is coming'
        )
        parser.add_argument(
            f'--{constants.KAFKA_BQ_TOPIC}',
            dest=constants.KAFKA_BQ_TOPIC,
            required=True,
            help='Kafka Topic Name'
        )
        parser.add_argument(
            f'--{constants.KAFKA_BQ_STARTING_OFFSET}',
            dest=constants.KAFKA_BQ_STARTING_OFFSET,
            required=True,
            help='Offset to start reading from. Accepted values: "earliest", "latest","{json string}"}'
        )
        parser.add_argument(
            f'--{constants.KAFKA_BQ_DATASET}',
            dest=constants.KAFKA_BQ_DATASET,
            required=True,
            help='Bigquery Dataset'
        )
        parser.add_argument(
            f'--{constants.KAFKA_BQ_TABLE_NAME}',
            dest=constants.KAFKA_BQ_TABLE_NAME,
            required=True,
            help="Bigquery Table Name"
        )
        parser.add_argument(
            f'--{constants.KAFKA_BQ_OUTPUT_MODE}',
            dest=constants.KAFKA_BQ_OUTPUT_MODE,
            required=True,
            help="Bigquery Table Output Mode (append , complete, update)"
        )
        parser.add_argument(
            f'--{constants.KAFKA_BQ_TEMP_BUCKET_NAME}',
            dest=constants.KAFKA_BQ_TEMP_BUCKET_NAME,
            required=True,
            help="GCS Temp Bucket Name"
        )
        parser.add_argument(
            f'--{constants.KAFKA_BQ_TERMINATION_TIMEOUT}',
            dest=constants.KAFKA_BQ_TERMINATION_TIMEOUT,
            required=True,
            help="Timeout for termination of kafka subscription"
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        ignore_keys = {constants.KAFKA_BOOTSTRAP_SERVERS}
        filtered_args = {key:val for key,val in args.items() if key not in ignore_keys}
        logger.info(
            "Starting Kafka to Bigquery Pyspark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
        )

        #arguments
        bootstrap_server_list: str = args[constants.KAFKA_BOOTSTRAP_SERVERS]
        checkpoint_location: str = args[constants.KAFKA_BQ_CHECKPOINT_LOCATION]
        kafka_topics: str= args[constants.KAFKA_BQ_TOPIC]
        big_query_dataset: str = args[constants.KAFKA_BQ_DATASET]
        big_query_table: str = args[constants.KAFKA_BQ_TABLE_NAME]
        bq_temp_bucket: str = args[constants.KAFKA_BQ_TEMP_BUCKET_NAME]
        timeout: int = int(args[constants.KAFKA_BQ_TERMINATION_TIMEOUT])
        offset:str = args[constants.KAFKA_BQ_STARTING_OFFSET]
        output_mode = args[constants.KAFKA_BQ_OUTPUT_MODE]

    
        df = spark.readStream.format(constants.KAFKA_INPUT_FORMAT) \
                  .option('kafka.bootstrap.servers', bootstrap_server_list) \
                  .option('subscribe', kafka_topics) \
                  .option('startingOffsets',offset) \
                  .load()
        
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


        # Write
        output = df \
        .writeStream \
        .format(constants.FORMAT_BIGQUERY) \
        .outputMode(output_mode) \
        .option('checkpointLocation',checkpoint_location) \
        .option('table',big_query_dataset+'.'+big_query_table) \
        .option('temporaryGcsBucket', bq_temp_bucket) \
        .start() 
        
        output.awaitTermination(timeout)




