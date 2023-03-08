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

from dataproc_templates.hive.hive_to_gcs import HiveToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestHiveToGCSTemplate:
    """
    Test suite for HiveToGCSTemplate
    """

    def test_parse_args(self):
        """Tests HiveToGCSTemplate.parse_args()"""

        hive_to_gcs_template = HiveToGCSTemplate()
        parsed_args = hive_to_gcs_template.parse_args(
            ["--hive.gcs.input.database=database",
             "--hive.gcs.input.table=table",
             "--hive.gcs.output.location=gs://test",
             "--hive.gcs.output.format=parquet",
             "--hive.gcs.output.mode=overwrite"])

        assert parsed_args["hive.gcs.input.database"] == "database"
        assert parsed_args["hive.gcs.input.table"] == "table"
        assert parsed_args["hive.gcs.output.location"] == "gs://test"
        assert parsed_args["hive.gcs.output.format"] == "parquet"
        assert parsed_args["hive.gcs.output.mode"] == "overwrite"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_parquet(self, mock_spark_session):
        """Tests HiveToGCSTemplate runs for parquet format output"""

        hive_to_gcs_template = HiveToGCSTemplate()
        mock_parsed_args = hive_to_gcs_template.parse_args(
            ["--hive.gcs.input.database=database",
             "--hive.gcs.input.table=table",
             "--hive.gcs.output.location=gs://test",
             "--hive.gcs.output.format=parquet",
             "--hive.gcs.output.mode=overwrite"])
        mock_spark_session.table.return_value = mock_spark_session.dataframe.DataFrame
        hive_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.table.assert_called_once_with("database.table")
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .parquet.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_avro(self, mock_spark_session):
        """Tests HiveToGCSTemplate runs for avro format output"""

        hive_to_gcs_template = HiveToGCSTemplate()
        mock_parsed_args = hive_to_gcs_template.parse_args(
            ["--hive.gcs.input.database=database",
             "--hive.gcs.input.table=table",
             "--hive.gcs.output.location=gs://test",
             "--hive.gcs.output.format=avro",
             "--hive.gcs.output.mode=append"])
        mock_spark_session.table.return_value = mock_spark_session.dataframe.DataFrame
        hive_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.table.assert_called_once_with("database.table")
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
        """Tests HiveToGCSTemplate runs for csv format output"""

        hive_to_gcs_template = HiveToGCSTemplate()
        mock_parsed_args = hive_to_gcs_template.parse_args(
            ["--hive.gcs.input.database=database",
             "--hive.gcs.input.table=table",
             "--hive.gcs.output.location=gs://test",
             "--hive.gcs.output.format=csv",
             "--hive.gcs.output.mode=ignore"])
        mock_spark_session.table.return_value = mock_spark_session.dataframe.DataFrame
        hive_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.table.assert_called_once_with("database.table")
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
        """Tests HiveToGCSTemplate runs with json format output"""

        hive_to_gcs_template = HiveToGCSTemplate()
        mock_parsed_args = hive_to_gcs_template.parse_args(
            ["--hive.gcs.input.database=database",
             "--hive.gcs.input.table=table",
             "--hive.gcs.output.location=gs://test",
             "--hive.gcs.output.format=json",
             "--hive.gcs.output.mode=errorifexists"])
        mock_spark_session.table.return_value = mock_spark_session.dataframe.DataFrame
        hive_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.table.assert_called_once_with("database.table")
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_ERRORIFEXISTS)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .json.assert_called_once_with("gs://test")
