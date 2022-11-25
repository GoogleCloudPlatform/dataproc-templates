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
import argparse
import pprint
import sys
from logging import Logger
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

from confluent_kafka.admin import ClusterMetadata
from pyspark.sql import DataFrame
from pyspark.sql import DataFrameWriter
from pyspark.sql import Row
from pyspark.sql import SparkSession

import dataproc_templates.util.template_constants as constants
from dataproc_templates import BaseTemplate
from dataproc_templates.kafka.utils import get_kafka_admin_client
from dataproc_templates.kafka.utils import get_schema_registry_client
from dataproc_templates.kafka.utils import gslv

__all__ = ["KafkaMetadataToGCSTemplate"]


class KafkaMetadataToGCSTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Kafka Metadata (Topics and Schemas) to GCS
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f"--{constants.KAFKAMETADATA_TO_GCS_BOOTSTRAP_SERVERS}",
            dest=constants.KAFKAMETADATA_TO_GCS_BOOTSTRAP_SERVERS,
            required=True,
            help="Kafka Bootstrap Servers list (host:port)"
        )

        parser.add_argument(
            f"--{constants.KAFKAMETADATA_TO_GCS_API_KEY}",
            dest=constants.KAFKAMETADATA_TO_GCS_API_KEY,
            required=False,
            help="Kafka API Key"
        )

        parser.add_argument(
            f"--{constants.KAFKAMETADATA_TO_GCS_API_SECRET}",
            dest=constants.KAFKAMETADATA_TO_GCS_API_SECRET,
            required=False,
            help="Kafka API Secret"
        )

        parser.add_argument(
            f"--{constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_ENDPOINT}",
            dest=constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_ENDPOINT,
            required=True,
            help="Schema Registry Endpoint URL"
        )

        parser.add_argument(
            f"--{constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_KEY}",
            dest=constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_KEY,
            required=False,
            help="Schema Registry API Key"
        )

        parser.add_argument(
            f"--{constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_SECRET}",
            dest=constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_SECRET,
            required=False,
            help="Schema Registry API Secret"
        )

        parser.add_argument(
            f"--{constants.KAFKAMETADATA_TO_GCS_OUTPUT_LOCATION}",
            dest=constants.KAFKAMETADATA_TO_GCS_OUTPUT_LOCATION,
            required=True,
            help="GCS location for output files"
        )

        parser.add_argument(
            f"--{constants.KAFKAMETADATA_TO_GCS_OUTPUT_FORMAT}",
            dest=constants.KAFKAMETADATA_TO_GCS_OUTPUT_FORMAT,
            required=False,
            default=constants.FORMAT_PRQT,
            help=(
                "Output file format "
                "(one of: avro,parquet,csv,json) "
                "(Defaults to parquet)"
            ),
            choices=[
                constants.FORMAT_AVRO,
                constants.FORMAT_PRQT,
                constants.FORMAT_CSV,
                constants.FORMAT_JSON
            ]
        )

        parser.add_argument(
            f"--{constants.KAFKAMETADATA_TO_GCS_OUTPUT_MODE}",
            dest=constants.KAFKAMETADATA_TO_GCS_OUTPUT_MODE,
            required=False,
            default=constants.OUTPUT_MODE_OVERWRITE,
            help=(
                "Output write mode "
                "(one of: append,overwrite,ignore,errorifexists) "
                "(Defaults to overwrite)"
            ),
            choices=[
                constants.OUTPUT_MODE_OVERWRITE,
                constants.OUTPUT_MODE_APPEND,
                constants.OUTPUT_MODE_IGNORE,
                constants.OUTPUT_MODE_ERRORIFEXISTS
            ]
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        if (getattr(known_args, constants.KAFKAMETADATA_TO_GCS_API_KEY)
            and not getattr(known_args,
                            constants.KAFKAMETADATA_TO_GCS_API_SECRET)) \
            or (getattr(known_args, constants.KAFKAMETADATA_TO_GCS_API_SECRET)
                and not getattr(known_args,
                                constants.KAFKAMETADATA_TO_GCS_API_KEY)):
            sys.exit(
                f'ArgumentParser Error: If you inform the {constants.KAFKAMETADATA_TO_GCS_API_KEY} or {constants.KAFKAMETADATA_TO_GCS_API_SECRET} both needs to be informed')

        if (getattr(known_args,
                    constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_KEY)
            and not getattr(known_args,
                            constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_SECRET)) \
            or (getattr(known_args,
                        constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_SECRET)
                and not getattr(known_args,
                                constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_KEY)):
            sys.exit(
                f'ArgumentParser Error: If you inform the {constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_KEY} or {constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_SECRET} both needs to be informed')

        return vars(known_args)

    def write(self,
        path: str = None,
        dataframe: DataFrame = None,
        mode: str = None,
        format: str = None):

        writer: DataFrameWriter = dataframe.write.mode(mode)

        if format == constants.FORMAT_AVRO:
            writer \
                .format(constants.FORMAT_AVRO) \
                .save(path)
        elif format == constants.FORMAT_CSV:
            writer \
                .option(constants.HEADER, True) \
                .csv(path)
        elif format == constants.FORMAT_JSON:
            writer.json(path)
        elif format == constants.FORMAT_PRQT:
            writer.parquet(path)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        kafka_bootstrap_servers: str = args[
            constants.KAFKAMETADATA_TO_GCS_BOOTSTRAP_SERVERS]
        kafka_api_key: str = args[constants.KAFKAMETADATA_TO_GCS_API_KEY]
        kafka_api_secret: str = args[constants.KAFKAMETADATA_TO_GCS_API_SECRET]

        schema_registry_endpoint: str = args[
            constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_ENDPOINT]
        schema_registry_api_key: str = args[
            constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_KEY]
        schema_registry_api_secret: str = args[
            constants.KAFKAMETADATA_TO_GCS_SCHEMAREGISTRY_API_SECRET]

        output_location: str = args[
            constants.KAFKAMETADATA_TO_GCS_OUTPUT_LOCATION]
        output_format: str = args[constants.KAFKAMETADATA_TO_GCS_OUTPUT_FORMAT]
        output_mode: str = args[constants.KAFKAMETADATA_TO_GCS_OUTPUT_MODE]

        logger.info(
            "Starting Kafka Metadata to GCS spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        kafka = get_kafka_admin_client(kafka_bootstrap_servers,
                                       kafka_api_key,
                                       kafka_api_secret)

        schema_registry = get_schema_registry_client(schema_registry_endpoint,
                                                     schema_registry_api_key,
                                                     schema_registry_api_secret)

        # Get Topics Metadata
        logger.info("Getting topics metadata ... ")

        cluster_metadata: ClusterMetadata = kafka.list_topics()

        topics_metadata = []
        for _, v in cluster_metadata.topics.items():
            # cluster_id, broker, topic, partition_count
            topics_metadata.append((cluster_metadata.cluster_id,
                                    cluster_metadata.orig_broker_name, v.topic,
                                    len(v.partitions)))

        topics_df = spark.createDataFrame(
            data=[Row(cluster_id=t[0], broker=t[1], topic=t[2],
                      parition_count=t[3]) for t in topics_metadata]
        )
        topics_df.createOrReplaceTempView("topics")

        # Get Subject/Schema Metadata
        logger.info("Getting subjects metadata ... ")
        subjects: List[str] = schema_registry.get_subjects()

        subjects_df = spark.createDataFrame(
            data=[Row(subject=s) for s in subjects]
        )

        subjects_df.createOrReplaceTempView("subjects")

        subjects_df.show()

        spark.udf.register('gslv', gslv)

        schemas_df = spark.sql(f"""
            select 
                t.*,
                s.subject as subject,
                gslv(s.subject, '{schema_registry_endpoint}', '{schema_registry_api_key}', '{schema_registry_api_secret}') as schema
            from topics t
                inner join subjects s on concat(t.topic, '-value') = s.subject
        """)

        self.write(
            path=f"{output_location}",
            dataframe=schemas_df,
            mode=output_mode,
            format=output_format
        )
