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

from dataproc_templates.gcs.gcs_to_gcs import GCSToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestGCSToGCSTemplate:
    """
    Test suite for GCSToGCSTemplate
    """

    def test_parse_args(self):
        gcs_to_gcs_template = GCSToGCSTemplate()
        parsed_args = gcs_to_gcs_template.parse_args(
            ["--gcs.to.gcs.input.location=gs://input",
             "--gcs.to.gcs.input.format=csv",
             "--gcs.to.gcs.temp.view.name=temp",
             "--gcs.to.gcs.sql.query=select * from temp",
             "--gcs.to.gcs.output.format=csv",
             "--gcs.to.gcs.output.mode=overwrite",
             "--gcs.to.gcs.output.partition.column=column",
             "--gcs.to.gcs.output.location=gs://output"])

        assert parsed_args["gcs.to.gcs.input.location"] == "gs://input"
        assert parsed_args["gcs.to.gcs.input.format"] == "csv"
        assert parsed_args["gcs.to.gcs.temp.view.name"] == "temp"
        assert parsed_args["gcs.to.gcs.sql.query"] == "select * from temp"
        assert parsed_args["gcs.to.gcs.output.format"] == "csv"
        assert parsed_args["gcs.to.gcs.output.mode"] == "overwrite"
        assert parsed_args["gcs.to.gcs.output.partition.column"] == "column"
        assert parsed_args["gcs.to.gcs.output.location"] == "gs://output"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_parquet(self, mock_spark_session):
        """Tests GCSToGCSTemplate runs with parquet format"""

        gcs_to_gcs_template = GCSToGCSTemplate()
        mock_parsed_args = gcs_to_gcs_template.parse_args(
            ["--gcs.to.gcs.input.location=gs://input",
             "--gcs.to.gcs.input.format=parquet",
             "--gcs.to.gcs.temp.view.name=temp",
             "--gcs.to.gcs.sql.query=select * from temp",
             "--gcs.to.gcs.output.format=parquet",
             "--gcs.to.gcs.output.mode=overwrite",
             "--gcs.to.gcs.output.partition.column=column",
             "--gcs.to.gcs.output.location=gs://output"])

        mock_spark_session.read.parquet.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.parquet.assert_called_once_with("gs://input")
        mock_spark_session.read.parquet().createOrReplaceTempView.assert_called_once_with("temp")
        mock_spark_session.sql.assert_called_once_with("select * from temp")
        mock_spark_session.sql().write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.sql().write \
            .mode() \
            .partitionBy.assert_called_once_with("column")
        mock_spark_session.sql().write \
            .mode() \
            .partitionBy() \
            .parquet.assert_called_once_with("gs://output")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_csv(self, mock_spark_session):
        """Tests GCSToGCSTemplate runs with parquet format"""

        gcs_to_gcs_template = GCSToGCSTemplate()
        mock_parsed_args = gcs_to_gcs_template.parse_args(
            ["--gcs.to.gcs.input.location=gs://input",
             "--gcs.to.gcs.input.format=csv",
             "--gcs.to.gcs.temp.view.name=temp",
             "--gcs.to.gcs.sql.query=select * from temp",
             "--gcs.to.gcs.output.format=csv",
             "--gcs.to.gcs.output.mode=overwrite",
             "--gcs.to.gcs.output.partition.column=column",
             "--gcs.to.gcs.output.location=gs://output"])

        mock_spark_session.read.csv.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_once_with(constants.FORMAT_CSV)
        mock_spark_session.read.format() \
            .options.assert_called_once_with(**{constants.CSV_HEADER: 'true',
                                                constants.CSV_INFER_SCHEMA: 'true'})
        mock_spark_session.read.format() \
            .options() \
            .load.assert_called_once_with("gs://input")
        mock_spark_session.read.format() \
            .options() \
            .load() \
            .createOrReplaceTempView.assert_called_once_with("temp")

        mock_spark_session.sql.assert_called_once_with("select * from temp")
        mock_spark_session.sql().write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.sql().write \
            .mode() \
            .partitionBy.assert_called_once_with("column")
        mock_spark_session.sql().write \
            .mode() \
            .partitionBy() \
            .options.assert_called_once_with(**{constants.CSV_HEADER: 'true'})

        mock_spark_session.sql().write \
            .mode() \
            .partitionBy() \
            .options() \
            .csv.assert_called_once_with("gs://output")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_avro(self, mock_spark_session):
        """Tests GCSToGCSTemplate runs with avro format"""

        gcs_to_gcs_template = GCSToGCSTemplate()
        mock_parsed_args = gcs_to_gcs_template.parse_args(
            ["--gcs.to.gcs.input.location=gs://input",
             "--gcs.to.gcs.input.format=avro",
             "--gcs.to.gcs.temp.view.name=temp",
             "--gcs.to.gcs.sql.query=select * from temp",
             "--gcs.to.gcs.output.format=avro",
             "--gcs.to.gcs.output.mode=overwrite",
             "--gcs.to.gcs.output.partition.column=column",
             "--gcs.to.gcs.output.location=gs://output"])

        mock_spark_session.read.csv.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_once_with(constants.FORMAT_AVRO)

        mock_spark_session.read.format() \
            .load.assert_called_once_with("gs://input")
        mock_spark_session.read.format() \
            .load() \
            .createOrReplaceTempView.assert_called_once_with("temp")

        mock_spark_session.sql.assert_called_once_with("select * from temp")
        mock_spark_session.sql().write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.sql().write \
            .mode() \
            .partitionBy.assert_called_once_with("column")
        mock_spark_session.sql().write \
            .mode() \
            .partitionBy() \
            .format.assert_called_once_with(constants.FORMAT_AVRO)

        mock_spark_session.sql().write \
            .mode() \
            .partitionBy() \
            .format() \
            .save.assert_called_once_with("gs://output")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_json(self, mock_spark_session):
        """Tests GCSToGCSTemplate runs with json format"""

        gcs_to_gcs_template = GCSToGCSTemplate()
        mock_parsed_args = gcs_to_gcs_template.parse_args(
            ["--gcs.to.gcs.input.location=gs://input",
             "--gcs.to.gcs.input.format=json",
             "--gcs.to.gcs.temp.view.name=temp",
             "--gcs.to.gcs.sql.query=select * from temp",
             "--gcs.to.gcs.output.format=json",
             "--gcs.to.gcs.output.mode=overwrite",
             "--gcs.to.gcs.output.partition.column=column",
             "--gcs.to.gcs.output.location=gs://output"])

        mock_spark_session.read.json.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.json.assert_called_once_with("gs://input")
        mock_spark_session.read.json().createOrReplaceTempView.assert_called_once_with("temp")
        mock_spark_session.sql.assert_called_once_with("select * from temp")
        mock_spark_session.sql().write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.sql().write \
            .mode() \
            .partitionBy.assert_called_once_with("column")
        mock_spark_session.sql().write \
            .mode() \
            .partitionBy() \
            .json.assert_called_once_with("gs://output")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_delta(self, mock_spark_session):
        """Tests GCSToGCSTemplate runs with delta format"""

        gcs_to_gcs_template = GCSToGCSTemplate()
        mock_parsed_args = gcs_to_gcs_template.parse_args(
            ["--gcs.to.gcs.input.location=gs://input",
             "--gcs.to.gcs.input.format=delta",
             "--gcs.to.gcs.temp.view.name=temp",
             "--gcs.to.gcs.sql.query=select * from temp",
             "--gcs.to.gcs.output.format=avro",
             "--gcs.to.gcs.output.mode=overwrite",
             "--gcs.to.gcs.output.partition.column=column",
             "--gcs.to.gcs.output.location=gs://output"])

        mock_spark_session.read.csv.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_once_with(constants.FORMAT_DELTA)

        mock_spark_session.read.format() \
            .load.assert_called_once_with("gs://input")
        mock_spark_session.read.format() \
            .load() \
            .createOrReplaceTempView.assert_called_once_with("temp")

        mock_spark_session.sql.assert_called_once_with("select * from temp")
        mock_spark_session.sql().write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.sql().write \
            .mode() \
            .partitionBy.assert_called_once_with("column")
        mock_spark_session.sql().write \
            .mode() \
            .partitionBy() \
            .format.assert_called_once_with(constants.FORMAT_AVRO)

        mock_spark_session.sql().write \
            .mode() \
            .partitionBy() \
            .format() \
            .save.assert_called_once_with("gs://output")
