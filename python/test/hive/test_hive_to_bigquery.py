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

from dataproc_templates.hive.hive_to_bigquery import HiveToBigQueryTemplate
import dataproc_templates.util.template_constants as constants


class TestHiveToBigQueryTemplate:
    """
    Test suite for HiveToBigQueryTemplate
    """

    def test_parse_args(self):
        """Tests HiveToBigQueryTemplate.parse_args()"""

        hive_to_bigquery_template = HiveToBigQueryTemplate()
        parsed_args = hive_to_bigquery_template.parse_args(
            ["--hive.bigquery.input.database=database",
            "--hive.bigquery.input.table=table",
             "--hive.bigquery.output.dataset=dataset",
             "--hive.bigquery.output.table=table",
             "--hive.bigquery.temp.bucket.name=bucket",
             "--hive.bigquery.output.mode=overwrite",
             "--hive.bigquery.temp.view.name=temp",
             "--hive.bigquery.sql.query='select * from temp'"])

        assert parsed_args["hive.bigquery.input.database"] == "database"
        assert parsed_args["hive.bigquery.input.table"] == "table"
        assert parsed_args["hive.bigquery.output.dataset"] == "dataset"
        assert parsed_args["hive.bigquery.output.table"] == "table"
        assert parsed_args["hive.bigquery.temp.bucket.name"] == "bucket"
        assert parsed_args["hive.bigquery.output.mode"] == "overwrite"
        assert parsed_args["hive.bigquery.temp.view.name"] == "temp"
        assert parsed_args["hive.bigquery.sql.query"] == "'select * from temp'"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_overwrite(self, mock_spark_session):
        """Tests HiveToBigQueryTemplate runs with overwrite mode"""

        hive_to_bigquery_template = HiveToBigQueryTemplate()
        mock_parsed_args = hive_to_bigquery_template.parse_args(
            ["--hive.bigquery.input.database=database",
            "--hive.bigquery.input.table=table",
             "--hive.bigquery.output.dataset=dataset",
             "--hive.bigquery.output.table=table",
             "--hive.bigquery.temp.bucket.name=bucket",
             "--hive.bigquery.output.mode=overwrite"])
        mock_spark_session.table.return_value = mock_spark_session.dataframe.DataFrame
        hive_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.table.assert_called_once_with("database.table")
        mock_spark_session.dataframe.DataFrame.write \
            .format.assert_called_once_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option.assert_called_once_with(constants.TEMP_GCS_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode() \
            .save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_append(self, mock_spark_session):
        """Tests HiveToBigQueryTemplate runs with append mode"""

        hive_to_bigquery_template = HiveToBigQueryTemplate()
        mock_parsed_args = hive_to_bigquery_template.parse_args(
            ["--hive.bigquery.input.database=database",
            "--hive.bigquery.input.table=table",
             "--hive.bigquery.output.dataset=dataset",
             "--hive.bigquery.output.table=table",
             "--hive.bigquery.temp.bucket.name=bucket",
             "--hive.bigquery.output.mode=append"])
        mock_spark_session.table.return_value = mock_spark_session.dataframe.DataFrame
        hive_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.table.assert_called_once_with("database.table")
        mock_spark_session.dataframe.DataFrame.write \
            .format.assert_called_once_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option.assert_called_once_with(constants.TEMP_GCS_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode() \
            .save.assert_called_once()
