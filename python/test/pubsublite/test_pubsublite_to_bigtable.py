"""
 * Copyright 2022 Google LLC
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

import mock
import pyspark.sql
import pyspark.sql.streaming

from dataproc_templates.pubsublite.pubsublite_to_bigtable import PubSubLiteToBigtableTemplate
import dataproc_templates.util.template_constants as constants


class TestPubSubLiteToBigtableTemplate:
    """
    Test suite for PubSubLiteToBigtableTemplate
    """

    def test_parse_args1(self):
        """Tests PubSubLiteToBigtableTemplate.parse_args()"""

        pubsublite_to_bigtable_template = PubSubLiteToBigtableTemplate()
        parsed_args = pubsublite_to_bigtable_template.parse_args(
            ["--pubsublite.bigtable.subscription.path=projects/gcp-project/locations/us-west1/subscriptions/psltobt-sub",
             "--pubsublite.bigtable.streaming.timeout=120",
             "--pubsublite.bigtable.streaming.trigger=2 seconds",
             "--pubsublite.bigtable.streaming.checkpoint.path=gs://temp-bucket/checkpoint",
             "--pubsublite.bigtable.output.project=my-project",
             "--pubsublite.bigtable.output.instance=bt-instance1",
             "--pubsublite.bigtable.output.table=output_table",
             "--pubsublite.bigtable.output.column.families=cf1, cf2, cf3",
             "--pubsublite.bigtable.output.max.versions=3"
             ])

        assert parsed_args["pubsublite.bigtable.subscription.path"] == "projects/gcp-project/locations/us-west1/subscriptions/psltobt-sub"
        assert parsed_args["pubsublite.bigtable.streaming.timeout"] == 120
        assert parsed_args["pubsublite.bigtable.streaming.trigger"] == "2 seconds"
        assert parsed_args["pubsublite.bigtable.streaming.checkpoint.path"] == "gs://temp-bucket/checkpoint"
        assert parsed_args["pubsublite.bigtable.output.project"] == "my-project"
        assert parsed_args["pubsublite.bigtable.output.instance"] == "bt-instance1"
        assert parsed_args["pubsublite.bigtable.output.table"] == "output_table"
        assert parsed_args["pubsublite.bigtable.output.column.families"] == "cf1, cf2, cf3"
        assert parsed_args["pubsublite.bigtable.output.max.versions"] == 3

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch.object(pyspark.sql, 'DataFrame')
    def test_run_pass_args2(self, mock_spark_session, mock_df):
        """Tests PubSubLiteToBigtableTemplate reads data as a Dataframe"""

        pubsublite_to_bigtable_template = PubSubLiteToBigtableTemplate()

        mock_parsed_args = pubsublite_to_bigtable_template.parse_args(
            ["--pubsublite.bigtable.subscription.path=projects/gcp-project/locations/us-west1/subscriptions/psltobt-sub",
             "--pubsublite.bigtable.streaming.timeout=120",
             "--pubsublite.bigtable.streaming.trigger=2 seconds",
             "--pubsublite.bigtable.streaming.checkpoint.path=gs://temp-bucket/checkpoint",
             "--pubsublite.bigtable.output.project=my-project",
             "--pubsublite.bigtable.output.instance=bt-instance1",
             "--pubsublite.bigtable.output.table=output_table",
             "--pubsublite.bigtable.output.column.families=cf1, cf2, cf3",
             "--pubsublite.bigtable.output.max.versions=3"
             ])

        pubsublite_to_bigtable_template.run(
            mock_spark_session, mock_parsed_args)

        reader = mock_spark_session.readStream

        reader \
            .format \
            .assert_called_once_with(constants.FORMAT_PUBSUBLITE)

        reader \
            .format() \
            .option \
            .assert_called_once_with(constants.PUBSUBLITE_SUBSCRIPTION, 'projects/gcp-project/locations/us-west1/subscriptions/psltobt-sub')

        reader \
            .format() \
            .option() \
            .load \
            .return_value = mock_df

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch.object(pyspark.sql, 'DataFrame')
    @mock.patch.object(pyspark.sql.streaming, 'StreamingQuery')
    def test_run_pass_args3(self, mock_spark_session, mock_df, mock_query):
        """Tests PubSubLiteToBigtableTemplate writes streaming query"""

        pubsublite_to_bigtable_template = PubSubLiteToBigtableTemplate()

        mock_parsed_args = pubsublite_to_bigtable_template.parse_args(
            ["--pubsublite.bigtable.subscription.path=projects/gcp-project/locations/us-west1/subscriptions/psltobt-sub",
             "--pubsublite.bigtable.streaming.timeout=120",
             "--pubsublite.bigtable.streaming.trigger=2 seconds",
             "--pubsublite.bigtable.streaming.checkpoint.path=gs://temp-bucket/checkpoint",
             "--pubsublite.bigtable.output.project=my-project",
             "--pubsublite.bigtable.output.instance=bt-instance1",
             "--pubsublite.bigtable.output.table=output_table",
             "--pubsublite.bigtable.output.column.families=cf1, cf2, cf3",
             "--pubsublite.bigtable.output.max.versions=3"
             ])

        pubsublite_to_bigtable_template.run(
            mock_spark_session, mock_parsed_args)

        writer = mock_df.writeStream

        writer \
            .foreachBatch() \
            .options() \
            .trigger() \
            .start \
            .return_value = mock_query
