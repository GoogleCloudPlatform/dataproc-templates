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
import pyspark

from dataproc_templates.pubsublite.pubsublite_to_bigquery import PubsubliteToBQTemplate
import dataproc_templates.util.template_constants as constants


class TestPubsubliteToBQTemplate:
    """
    Test suite for PubsubliteToBQTemplate
    """

    def test_parse_args1(self):
        """Tests PubsubliteToBQTemplate.parse_args()"""

        pubsublite_to_bq_template = PubsubliteToBQTemplate()
        parsed_args = pubsublite_to_bq_template.parse_args(
            ["--pubsublite.to.bq.input.subscription.url=url",
             "--pubsublite.to.bq.project.id=projectID",
             "--pubsublite.to.bq.output.dataset=dataset1",
             "--pubsublite.to.bq.output.table=table1",
             "--pubsublite.to.bq.write.mode=overwrite",
             "--pubsublite.to.bq.temp.bucket.name=bucket",
             "--pubsublite.to.bq.checkpoint.location=gs://test"
             ])       
        
        assert parsed_args["pubsublite.to.bq.input.subscription.url"] == "url"
        assert parsed_args["pubsublite.to.bq.project.id"] == "projectID"
        assert parsed_args["pubsublite.to.bq.output.dataset"] == "dataset1"
        assert parsed_args["pubsublite.to.bq.output.table"] == "table1"
        assert parsed_args["pubsublite.to.bq.write.mode"] == "overwrite"  
        assert parsed_args["pubsublite.to.bq.temp.bucket.name"] == "bucket"   
        assert parsed_args["pubsublite.to.bq.checkpoint.location"] == "gs://test"   

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch.object(pyspark.sql, 'DataFrame')
    def test_run_pass_args2(self, mock_spark_session, mock_df):
        """Tests PubsubliteToBQTemplate reads data as a Dataframe"""

        pubsublite_to_bigquery_template = PubsubliteToBQTemplate()

        mock_parsed_args = pubsublite_to_bigquery_template.parse_args([
            "--pubsublite.to.bq.input.subscription.url=url",
             "--pubsublite.to.bq.project.id=projectID",
             "--pubsublite.to.bq.output.dataset=dataset1",
             "--pubsublite.to.bq.output.table=table1",
             "--pubsublite.to.bq.write.mode=overwrite",
             "--pubsublite.to.bq.temp.bucket.name=bucket",
             "--pubsublite.to.bq.checkpoint.location=gs://test",
        ])

        pubsublite_to_bigquery_template.run(
            mock_spark_session, mock_parsed_args)

        reader = mock_spark_session.readStream

        reader \
            .format \
            .assert_called_once_with(constants.FORMAT_PUBSUBLITE)

        reader \
            .format() \
            .option \
            .assert_called_once_with(f"{constants.FORMAT_PUBSUBLITE}.subscription", 'url')

        reader \
            .format() \
            .option() \
            .load \
            .return_value = mock_df

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch.object(pyspark.sql, 'DataFrame')
    @mock.patch.object(pyspark.sql.streaming, 'StreamingQuery')
    def test_run_pass_args3(self, mock_spark_session, mock_df, mock_query):
        """Tests PubsubliteToBQTemplate writes data to BigQuery"""

        pubsublite_to_bigquery_template = PubsubliteToBQTemplate()

        mock_parsed_args = pubsublite_to_bigquery_template.parse_args([
            "--pubsublite.to.bq.input.subscription.url=url",
             "--pubsublite.to.bq.project.id=projectID",
             "--pubsublite.to.bq.output.dataset=dataset1",
             "--pubsublite.to.bq.output.table=table1",
             "--pubsublite.to.bq.write.mode=overwrite",
             "--pubsublite.to.bq.temp.bucket.name=bucket",
             "--pubsublite.to.bq.checkpoint.location=gs://test",
             "--pubsublite.to.bq.processing.time=1 second",
             "--pubsublite.to.bq.input.timeout.sec=120"
        ])

        pubsublite_to_bigquery_template.run(
            mock_spark_session, mock_parsed_args)

        writer = mock_df.writeStream

        writer \
            .format(constants.FORMAT_BIGQUERY) \
            .return_value = writer

        writer \
            .format \
            .option(constants.PUBSUBLITE_TO_BQ_TEMPORARY_BUCKET, mock_parsed_args["pubsublite.to.bq.temp.bucket.name"]) \
            .return_value = writer

        writer \
            .format \
            .option \
            .option(constants.PUBSUBLITE_TO_BQ_CHECKPOINT_LOCATION, mock_parsed_args["pubsublite.to.bq.checkpoint.location"]) \
            .return_value = writer

        writer \
            .format \
            .option \
            .option \
            .option(constants.PUBSUBLITE_TO_BQ_OUTPUT_TABLE, mock_parsed_args["pubsublite.to.bq.output.table"]) \
            .return_value = writer

        writer \
            .format \
            .option \
            .option \
            .option \
            .trigger(constants.PUBSUBLITE_TO_BQ_PROCESSING_TIME, mock_parsed_args["pubsublite.to.bq.processing.time"]) \
            .return_value = writer

        writer \
            .format \
            .option \
            .option \
            .option \
            .trigger \
            .start() \
            .return_value = mock_query

        mock_query \
            .awaitTermination(constants.PUBSUBLITE_TO_BQ_INPUT_TIMEOUT_SEC, mock_parsed_args["pubsublite.to.bq.input.timeout.sec"]) 

        mock_query.stop()


        # Verify that the correct methods were called

        writer \
            .format \
            .assert_called_once_with(constants.FORMAT_BIGQUERY)

        writer \
            .format \
            .option \
            .assert_called_once_with(constants.PUBSUBLITE_TO_BQ_TEMPORARY_BUCKET, "bucket")

        writer \
            .format \
            .option \
            .option \
            .assert_called_once_with(constants.PUBSUBLITE_TO_BQ_CHECKPOINT_LOCATION, "gs://test")

        writer \
            .format \
            .option \
            .option \
            .option \
            .assert_called_once_with(constants.PUBSUBLITE_TO_BQ_OUTPUT_TABLE, "table1")

        writer \
            .format \
            .option \
            .option \
            .option \
            .trigger \
            .assert_called_once_with(constants.PUBSUBLITE_TO_BQ_PROCESSING_TIME, "1 second")

        writer \
            .format \
            .option \
            .option \
            .option \
            .trigger \
            .start \
            .assert_called_once_with()

        mock_query \
            .awaitTermination \
            .assert_called_once_with(constants.PUBSUBLITE_TO_BQ_INPUT_TIMEOUT_SEC, "120")

        mock_query \
            .stop \
            .assert_called_once_with()
        

