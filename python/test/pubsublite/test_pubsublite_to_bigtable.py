"""
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""

import sys
import mock
import pytest
import logging
import pyspark.sql.streaming
import dataproc_templates.util.template_constants as constants

from datetime import datetime
from importlib import reload

class TestPubSubLiteToBigtableTemplate():
    """
    Test suite for PubSubLiteToBigtableTemplate
    """

    @pytest.fixture(autouse=True)
    def pre_setup(self):

        with mock.patch('google.cloud.bigtable.Client') as mock_client:
            reload(sys.modules['dataproc_templates.pubsublite.pubsublite_to_bigtable'])
            from dataproc_templates.pubsublite.pubsublite_to_bigtable import (
                PubSubLiteToBigtableTemplate,
            )
            mock_client.return_value.instance.return_value.table.return_value = mock.MagicMock()
            pubsublite_to_bigtable_template = PubSubLiteToBigtableTemplate()
            mock_parsed_args = pubsublite_to_bigtable_template.parse_args(
                [
                    "--pubsublite.bigtable.subscription.path=projects/gcp-project/locations/us-west1/subscriptions/psltobt-sub",
                    "--pubsublite.bigtable.streaming.timeout=120",
                    "--pubsublite.bigtable.streaming.trigger=2 seconds",
                    "--pubsublite.bigtable.streaming.checkpoint.path=gs://temp-bucket/checkpoint",
                    "--pubsublite.bigtable.output.project=my-project",
                    "--pubsublite.bigtable.output.instance=bt-instance1",
                    "--pubsublite.bigtable.output.table=output_table",
                    "--pubsublite.bigtable.output.column.families=cf1, cf2, cf3",
                    "--pubsublite.bigtable.output.max.versions=3",
                ]
            )
            yield mock_client, pubsublite_to_bigtable_template, mock_parsed_args


    def test_parse_args(self, pre_setup):
        """Tests PubSubLiteToBigtableTemplate.parse_args()"""

        _, _, mock_parsed_args = pre_setup

        assert (
            mock_parsed_args["pubsublite.bigtable.subscription.path"]
            == "projects/gcp-project/locations/us-west1/subscriptions/psltobt-sub"
        )
        assert mock_parsed_args["pubsublite.bigtable.streaming.timeout"] == 120
        assert mock_parsed_args["pubsublite.bigtable.streaming.trigger"] == "2 seconds"
        assert (
            mock_parsed_args["pubsublite.bigtable.streaming.checkpoint.path"]
            == "gs://temp-bucket/checkpoint"
        )
        assert mock_parsed_args["pubsublite.bigtable.output.project"] == "my-project"
        assert mock_parsed_args["pubsublite.bigtable.output.instance"] == "bt-instance1"
        assert mock_parsed_args["pubsublite.bigtable.output.table"] == "output_table"
        assert (
            mock_parsed_args["pubsublite.bigtable.output.column.families"] == "cf1, cf2, cf3"
        )
        assert mock_parsed_args["pubsublite.bigtable.output.max.versions"] == 3

    @mock.patch.object(pyspark.sql, "SparkSession")
    @mock.patch.object(pyspark.sql, "DataFrame")
    def test_run_read_stream(self, mock_spark_session, mock_df, pre_setup):
        """Tests PubSubLiteToBigtableTemplate reads stream data as a Dataframe"""

        mock_client, pubsublite_to_bigtable_template, mock_parsed_args = pre_setup
        mock_reader = mock_spark_session.readStream

        pubsublite_to_bigtable_template.run(mock_spark_session, mock_parsed_args)

        # Assert
        mock_reader.format.assert_called_once_with(constants.FORMAT_PUBSUBLITE)
        mock_reader.format().option.assert_called_once_with(
            constants.PUBSUBLITE_SUBSCRIPTION,
            "projects/gcp-project/locations/us-west1/subscriptions/psltobt-sub",
        )
        mock_reader.format().option().load.return_value = mock_df

    @mock.patch.object(logging, "Logger")
    def test_get_table(self, mock_logger, pre_setup):
        """Tests PubSubLiteToBigtableTemplate writes stream data to Bigtable"""

        mock_client, pubsublite_to_bigtable_template, _ = pre_setup

        mock_instance_id = "bt-instance1"
        mock_table_id = "output_table"
        mock_cf_list = ""
        mock_max_versions = "3"

        mock_table = pubsublite_to_bigtable_template.get_table(
            mock_client,
            mock_instance_id,
            mock_table_id,
            mock_cf_list,
            mock_max_versions,
            mock_logger,
        )

        # Assert
        mock_table.exists.assert_called_once()
        mock_logger.info.assert_called_once_with("Table output_table already exists.")

    @mock.patch.object(pyspark.sql, "DataFrame")
    @mock.patch.object(logging, "Logger")
    def test_populate_table(self, mock_batch_df, mock_logger, pre_setup):
        """Tests PubSubLiteToBigtableTemplate writes stream data to Bigtable"""

        mock_client, pubsublite_to_bigtable_template, mock_parsed_args = pre_setup
        mock_table = mock.MagicMock()

        pubsublite_to_bigtable_template.populate_table(
            mock_batch_df,
            mock_table,
            mock_logger,
        )
        mock_message_data = {
            "rowkey": "rk1",
            "columns": [
                {
                    "columnfamily": "place",
                    "columnname": "city",
                    "columnvalue": "Bangalore",
                }
            ],
        }
        mock_timestamp = datetime(2023, 4, 4, 14, 26, 6, 122642)
        mock_row = mock_table.direct_row(mock_message_data["rowkey"])
        for mock_cell in mock_message_data["columns"]:
            mock_cf = mock_cell["columnfamily"]
            mock_name = mock_cell["columnname"]
            mock_value = mock_cell["columnvalue"]
            mock_row.set_cell(mock_cf, mock_name, mock_value, mock_timestamp)

        # Assert
        mock_logger.info.assert_called_once_with("Writing input data to the table.")
        mock_batch_df.collect.assert_called_once_with()
        mock_table.direct_row.assert_called_once_with("rk1")
        mock_row.set_cell.assert_called_once_with(
            "place", "city", "Bangalore", datetime(2023, 4, 4, 14, 26, 6, 122642)
        )
        mock_table.mutate_rows.assert_called_once_with([])

    @mock.patch.object(pyspark.sql, "SparkSession")
    @mock.patch.object(pyspark.sql, "DataFrame")
    @mock.patch.object(pyspark.sql.streaming, "StreamingQuery")
    def test_run_write_stream(self, mock_spark_session, mock_df, mock_query, pre_setup):
        """Tests PubSubLiteToBigtableTemplate writes stream data to Bigtable"""

        # Setup
        mock_client, pubsublite_to_bigtable_template, mock_parsed_args = pre_setup

        mock_writer = mock_df.writeStream
        mock_writer.foreachBatch().return_value = mock_writer
        mock_writer.foreachBatch.options(
            {
                constants.PUBSUBLITE_CHECKPOINT_LOCATION: mock_parsed_args[
                    "pubsublite.bigtable.streaming.checkpoint.path"
                ]
            }
        ).return_value = mock_writer
        mock_writer.foreachBatch.options.trigger(
            processingTime=mock_parsed_args["pubsublite.bigtable.streaming.trigger"]
        ).return_value = mock_writer

        mock_writer.foreachBatch.options.trigger.start().return_value = mock_query

        pubsublite_to_bigtable_template.run(mock_spark_session, mock_parsed_args)

        mock_query.awaitTermination(
            mock_parsed_args["pubsublite.bigtable.streaming.timeout"]
        )
        mock_query.stop()

        # Assert
        mock_writer.foreachBatch.assert_called_once_with()
        mock_writer.foreachBatch.options.assert_called_once_with(
            {"checkpointLocation": "gs://temp-bucket/checkpoint"}
        )
        mock_writer.foreachBatch.options.trigger.assert_called_once_with(
            processingTime="2 seconds"
        )
        mock_writer.foreachBatch.options.trigger.start.assert_called_once_with()
        mock_query.awaitTermination.assert_called_once_with(120)
        mock_query.stop.assert_called_once_with()
