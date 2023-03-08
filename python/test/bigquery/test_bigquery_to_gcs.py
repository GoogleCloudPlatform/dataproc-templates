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

from dataproc_templates.bigquery.bigquery_to_gcs import BigQueryToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestBigQueryToGCSTemplate:
    """
    Test suite for BigQueryToGCSTemplate
    """

    def test_parse_args(self):
        """Tests BigQueryToGCSTemplate.parse_args()"""

        bigquery_to_gcs_template = BigQueryToGCSTemplate()
        parsed_args = bigquery_to_gcs_template.parse_args(
            ["--bigquery.gcs.input.table=projectId:dataset.table",
             "--bigquery.gcs.output.format=parquet",
             "--bigquery.gcs.output.mode=overwrite",
             "--bigquery.gcs.output.location=gs://test"])

        assert parsed_args["bigquery.gcs.input.table"] == "projectId:dataset.table"
        assert parsed_args["bigquery.gcs.output.format"] == "parquet"
        assert parsed_args["bigquery.gcs.output.mode"] == "overwrite"
        assert parsed_args["bigquery.gcs.output.location"] == "gs://test"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_parquet(self, mock_spark_session):
        """Tests BigQueryToGCSTemplate runs for parquet format output"""

        bigquery_to_gcs_template = BigQueryToGCSTemplate()
        mock_parsed_args = bigquery_to_gcs_template.parse_args(
            ["--bigquery.gcs.input.table=projectId:dataset.table",
             "--bigquery.gcs.output.format=parquet",
             "--bigquery.gcs.output.mode=overwrite",
             "--bigquery.gcs.output.location=gs://test"])
        mock_spark_session.read.format().option().load.return_value \
            = mock_spark_session.dataframe.DataFrame
        bigquery_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format.assert_called_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.read \
            .format() \
            .option.assert_called_with(constants.TABLE,"projectId:dataset.table")
        mock_spark_session.read \
            .format() \
            .option() \
            .load.assert_called_with()
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .parquet.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_avro(self, mock_spark_session):
        """Tests BigQueryToGCSTemplate runs for avro format output"""

        bigquery_to_gcs_template = BigQueryToGCSTemplate()
        mock_parsed_args = bigquery_to_gcs_template.parse_args(
            ["--bigquery.gcs.input.table=projectId:dataset.table",
             "--bigquery.gcs.output.format=avro",
             "--bigquery.gcs.output.mode=append",
             "--bigquery.gcs.output.location=gs://test"])
        mock_spark_session.read.format().option().load.return_value \
            = mock_spark_session.dataframe.DataFrame
        bigquery_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format.assert_called_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.read \
            .format() \
            .option.assert_called_with(constants.TABLE, "projectId:dataset.table")
        mock_spark_session.read \
            .format() \
            .option() \
            .load.assert_called_once()
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .format.assert_called_once_with(constants.FORMAT_AVRO)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .format() \
            .save.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_csv(self, mock_spark_session):
        """Tests BigQueryToGCSTemplate runs for csv format output"""

        bigquery_to_gcs_template = BigQueryToGCSTemplate()
        mock_parsed_args = bigquery_to_gcs_template.parse_args(
            ["--bigquery.gcs.input.table=projectId:dataset.table",
             "--bigquery.gcs.output.format=csv",
             "--bigquery.gcs.output.mode=ignore",
             "--bigquery.gcs.output.location=gs://test"])
        mock_spark_session.read.format().option().load.return_value \
            = mock_spark_session.dataframe.DataFrame
        bigquery_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format.assert_called_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.read \
            .format() \
            .option.assert_called_with(constants.TABLE, "projectId:dataset.table")
        mock_spark_session.read \
            .format() \
            .option() \
            .load.assert_called_once()
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_IGNORE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .options.assert_called_once_with(**{constants.CSV_HEADER: 'true'})
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .options() \
            .csv.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_json(self, mock_spark_session):
        """Tests BigQueryToGCSTemplate runs with json format output"""

        bigquery_to_gcs_template = BigQueryToGCSTemplate()
        mock_parsed_args = bigquery_to_gcs_template.parse_args(
            ["--bigquery.gcs.input.table=projectId:dataset.table",
             "--bigquery.gcs.output.format=json",
             "--bigquery.gcs.output.mode=errorifexists",
             "--bigquery.gcs.output.location=gs://test"])
        mock_spark_session.read.format().option().load.return_value \
            = mock_spark_session.dataframe.DataFrame
        bigquery_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format.assert_called_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.read \
            .format() \
            .option.assert_called_with(constants.TABLE, "projectId:dataset.table")
        mock_spark_session.read \
            .format() \
            .option() \
            .load.assert_called_once()
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_ERRORIFEXISTS)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .json.assert_called_once_with("gs://test")
