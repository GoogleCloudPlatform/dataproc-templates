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
from datetime import datetime
from json import loads

from google.cloud.bigtable import Client, column_family
from google.cloud.bigtable.table import Table
from google.cloud.bigtable.row import DirectRow

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants


__all__ = ["PubSubLiteToBigtableTemplate"]


class PubSubLiteToBigtableTemplate(BaseTemplate):

    """
    Dataproc template implementing exports from Pub/Sub Lite to Bigtable
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f"--{constants.PUBSUBLITE_BIGTABLE_SUBSCRIPTION_PATH}",
            dest=constants.PUBSUBLITE_BIGTABLE_SUBSCRIPTION_PATH,
            required=True,
            help="Pub/Sub Lite subscription path",
        )

        parser.add_argument(
            f"--{constants.PUBSUBLITE_BIGTABLE_STREAMING_TIMEOUT}",
            dest=constants.PUBSUBLITE_BIGTABLE_STREAMING_TIMEOUT,
            type=int,
            default=60,
            required=False,
            help="Time duration after which the streaming query will be stopped (in seconds)",
        )

        parser.add_argument(
            f"--{constants.PUBSUBLITE_BIGTABLE_STREAMING_TRIGGER}",
            dest=constants.PUBSUBLITE_BIGTABLE_STREAMING_TRIGGER,
            default="0 seconds",
            required=False,
            help="Time interval at which the streaming query runs to process incoming data",
        )

        parser.add_argument(
            f"--{constants.PUBSUBLITE_BIGTABLE_STREAMING_CHECKPOINT_PATH}",
            dest=constants.PUBSUBLITE_BIGTABLE_STREAMING_CHECKPOINT_PATH,
            required=False,
            help="Temporary folder path to store checkpoint information",
        )

        parser.add_argument(
            f"--{constants.PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT}",
            dest=constants.PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT,
            required=True,
            help="GCP project containing the Bigtable instance",
        )

        parser.add_argument(
            f"--{constants.PUBSUBLITE_BIGTABLE_OUTPUT_INSTANCE}",
            dest=constants.PUBSUBLITE_BIGTABLE_OUTPUT_INSTANCE,
            required=True,
            help="Bigtable instance ID, containing the output table",
        )

        parser.add_argument(
            f"--{constants.PUBSUBLITE_BIGTABLE_OUTPUT_TABLE}",
            dest=constants.PUBSUBLITE_BIGTABLE_OUTPUT_TABLE,
            required=True,
            help="Table ID in Bigtable, to store the output",
        )

        parser.add_argument(
            f"--{constants.PUBSUBLITE_BIGTABLE_OUTPUT_COLUMN_FAMILIES}",
            dest=constants.PUBSUBLITE_BIGTABLE_OUTPUT_COLUMN_FAMILIES,
            required=False,
            help="List of Column Family names to create a new table",
        )

        parser.add_argument(
            f"--{constants.PUBSUBLITE_BIGTABLE_OUTPUT_MAX_VERSIONS}",
            dest=constants.PUBSUBLITE_BIGTABLE_OUTPUT_MAX_VERSIONS,
            default=1,
            type=int,
            required=False,
            help="Maximum number of versions of cells in the new table (Garbage Collection Policy)",
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def get_table(
        self,
        client: Client,
        instance_id: str,
        table_id: str,
        column_families_list: str,
        max_versions: str,
        logger: Logger,
    ) -> Table:
        """
        Checks if table exists in Bigtable or tries to create one
        """

        table = client.instance(instance_id).table(table_id)
        if table.exists():
            logger.info(f"Table {table_id} already exists.")
        else:
            if column_families_list:
                logger.info(f"Table {table_id} does not exist. Creating {table_id}")

                max_versions_rule = column_family.MaxVersionsGCRule(max_versions)
                column_families_itr = list(
                    map(str.strip, column_families_list.split(","))
                )
                column_families = dict.fromkeys(column_families_itr, max_versions_rule)
                table.create(column_families=column_families)
            else:
                raise RuntimeError(
                    f"Table {table_id} does not exist, provide column families to create it"
                )

        return table

    def populate_table(self, batch_df: DataFrame, table: Table, logger: Logger) -> None:
        """
        Writes data to Bigtable instance
        """

        # Write to table
        logger.info("Writing input data to the table.")

        rows: list[DirectRow] = []
        for row in batch_df.collect():
            message_data = loads(row.data)
            row_key = message_data["rowkey"]
            new_row = table.direct_row(row_key)

            for cell in message_data["columns"]:
                new_row.set_cell(
                    column_family_id=cell["columnfamily"],
                    column=cell["columnname"],
                    value=cell["columnvalue"],
                    timestamp=datetime.utcnow(),
                )
            rows.append(new_row)
        table.mutate_rows(rows)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:
        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        subscription_path: str = args[constants.PUBSUBLITE_BIGTABLE_SUBSCRIPTION_PATH]
        timeout: int = args[constants.PUBSUBLITE_BIGTABLE_STREAMING_TIMEOUT]
        trigger: str = args[constants.PUBSUBLITE_BIGTABLE_STREAMING_TRIGGER]
        checkpoint_location: str = args[constants.PUBSUBLITE_BIGTABLE_STREAMING_CHECKPOINT_PATH]
        project: str = args[constants.PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT]
        instance_id: str = args[constants.PUBSUBLITE_BIGTABLE_OUTPUT_INSTANCE]
        table_id: str = args[constants.PUBSUBLITE_BIGTABLE_OUTPUT_TABLE]
        column_families_list: str = args[constants.PUBSUBLITE_BIGTABLE_OUTPUT_COLUMN_FAMILIES]
        max_versions: str = args[constants.PUBSUBLITE_BIGTABLE_OUTPUT_MAX_VERSIONS]

        ignore_keys = {
            constants.PUBSUBLITE_BIGTABLE_SUBSCRIPTION_PATH,
            constants.PUBSUBLITE_BIGTABLE_STREAMING_CHECKPOINT_PATH,
            constants.PUBSUBLITE_BIGTABLE_OUTPUT_PROJECT,
        }
        filtered_args = {
            key: val for key, val in args.items() if key not in ignore_keys
        }
        logger.info(
            "Starting Pub/Sub Lite to Bigtable spark job with parameters:\n"
            f"{pprint.pformat(filtered_args)}"
        )

        # Read
        input_data: DataFrame

        input_data = (
            spark.readStream.format(constants.FORMAT_PUBSUBLITE)
            .option(constants.PUBSUBLITE_SUBSCRIPTION, subscription_path)
            .load()
        )

        input_data = input_data.withColumn("data", input_data.data.cast(StringType()))

        # Write
        options = {}
        if checkpoint_location:
            options = {constants.PUBSUBLITE_CHECKPOINT_LOCATION: checkpoint_location}

        client = Client(project=project, admin=True)
        table = self.get_table(
            client,
            instance_id,
            table_id,
            column_families_list,
            max_versions,
            logger,
        )

        def write_to_bigtable(batch_df: DataFrame, batch_id: int):
            self.populate_table(batch_df, table, logger)

        query = (
            input_data.writeStream.foreachBatch(write_to_bigtable)
            .options(**options)
            .trigger(processingTime=trigger)
            .start()
        )
        query.awaitTermination(timeout)
        query.stop()

        client.close()
