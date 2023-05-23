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

from dataproc_templates.gcs.gcs_to_jdbc import GCSToJDBCTemplate
import dataproc_templates.util.template_constants as constants


class TestGCSToBigQueryTemplate:
    """
    Test suite for GCSToJDBCTemplate
    """

    def test_parse_args(self):
        """Tests GCSToJDBCTemplate.parse_args()"""

        gcs_to_jdbc_template = GCSToJDBCTemplate()
        parsed_args = gcs_to_jdbc_template.parse_args(
            ["--gcs.jdbc.input.format=parquet",
             "--gcs.jdbc.input.location=gs://test",
             "--gcs.jdbc.output.url=url",
             "--gcs.jdbc.output.table=table",
             "--gcs.jdbc.output.driver=driver",
             "--gcs.jdbc.output.mode=append"])

        assert parsed_args["gcs.jdbc.input.format"] == "parquet"
        assert parsed_args["gcs.jdbc.input.location"] == "gs://test"
        assert parsed_args["gcs.jdbc.output.table"] == "table"
        assert parsed_args["gcs.jdbc.output.mode"] == "append"
        assert parsed_args["gcs.jdbc.output.driver"] == "driver"
        assert parsed_args["gcs.jdbc.output.url"] == "url"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_parquet(self, mock_spark_session):
        """Tests GCSToJDBCTemplate runs with parquet format"""

        gcs_to_jdbc_template = GCSToJDBCTemplate()
        mock_parsed_args = gcs_to_jdbc_template.parse_args(
            ["--gcs.jdbc.input.format=parquet",
             "--gcs.jdbc.input.location=gs://test",
             "--gcs.jdbc.output.url=url",
             "--gcs.jdbc.output.table=table",
             "--gcs.jdbc.output.driver=driver",
             "--gcs.jdbc.output.mode=append"])
        mock_spark_session.read.parquet.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_jdbc_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.parquet.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_JDBC)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option.assert_called_once_with(constants.JDBC_TABLE, "table")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_avro(self, mock_spark_session):
        """Tests GCSToJDBCTemplate runs with avro"""

        gcs_to_jdbc_template = GCSToJDBCTemplate()
        mock_parsed_args = gcs_to_jdbc_template.parse_args(
            ["--gcs.jdbc.input.format=avro",
             "--gcs.jdbc.input.location=gs://test",
             "--gcs.jdbc.output.url=url",
             "--gcs.jdbc.output.table=table",
             "--gcs.jdbc.output.driver=driver",
             "--gcs.jdbc.output.mode=append"])
        mock_spark_session.read.format().load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_jdbc_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_AVRO_EXTD)
        mock_spark_session.read.format().load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_JDBC)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option.assert_called_once_with(constants.JDBC_TABLE, "table")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_json(self, mock_spark_session):
        """Tests GCSToJDBCTemplate runs with json format"""

        gcs_to_jdbc_template = GCSToJDBCTemplate()
        mock_parsed_args = gcs_to_jdbc_template.parse_args(
            ["--gcs.jdbc.input.format=json",
             "--gcs.jdbc.input.location=gs://test",
             "--gcs.jdbc.output.url=url",
             "--gcs.jdbc.output.table=table",
             "--gcs.jdbc.output.driver=driver",
             "--gcs.jdbc.output.mode=append"])
        mock_spark_session.read.json.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_jdbc_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.json.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_JDBC)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option.assert_called_once_with(constants.JDBC_TABLE, "table")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_delta(self, mock_spark_session):
        """Tests GCSToJDBCTemplate runs with delta"""

        gcs_to_jdbc_template = GCSToJDBCTemplate()
        mock_parsed_args = gcs_to_jdbc_template.parse_args(
            ["--gcs.jdbc.input.format=delta",
             "--gcs.jdbc.input.location=gs://test",
             "--gcs.jdbc.output.url=url",
             "--gcs.jdbc.output.table=table",
             "--gcs.jdbc.output.driver=driver",
             "--gcs.jdbc.output.mode=append"])
        mock_spark_session.read.format().load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_jdbc_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_DELTA)
        mock_spark_session.read.format().load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_JDBC)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option.assert_called_once_with(constants.JDBC_TABLE, "table")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_csv1(self, mock_spark_session):
        """Tests GCSToJDBCTemplate runs with csv format"""

        gcs_to_jdbc_template = GCSToJDBCTemplate()
        mock_parsed_args = gcs_to_jdbc_template.parse_args(
            ["--gcs.jdbc.input.format=csv",
             "--gcs.jdbc.input.location=gs://test",
             "--gcs.jdbc.input.header=false",
             "--gcs.jdbc.output.url=url",
             "--gcs.jdbc.output.table=table",
             "--gcs.jdbc.output.driver=driver",
             "--gcs.jdbc.output.mode=append"])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_jdbc_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_CSV)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.CSV_HEADER: 'false',
            constants.CSV_INFER_SCHEMA: 'true',
        })
        mock_spark_session.read.format().options().load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_JDBC)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option.assert_called_once_with(constants.JDBC_TABLE, "table")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_csv2(self, mock_spark_session):
        """Tests GCSToJDBCTemplate runs with csv format and some optional csv options"""

        gcs_to_jdbc_template = GCSToJDBCTemplate()
        mock_parsed_args = gcs_to_jdbc_template.parse_args(
            ["--gcs.jdbc.input.format=csv",
             "--gcs.jdbc.input.location=gs://test",
             "--gcs.jdbc.input.inferschema=false",
             "--gcs.jdbc.input.sep=|",
             "--gcs.jdbc.input.comment=#",
             "--gcs.jdbc.input.timestampntzformat=yyyy-MM-dd'T'HH:mm:ss",
             "--gcs.jdbc.output.url=url",
             "--gcs.jdbc.output.table=table",
             "--gcs.jdbc.output.driver=driver",
             "--gcs.jdbc.output.mode=append"])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_jdbc_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_CSV)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.CSV_HEADER: 'true',
            constants.CSV_INFER_SCHEMA: 'false',
            constants.CSV_SEP: "|",
            constants.CSV_COMMENT: "#",
            constants.CSV_TIMESTAMPNTZFORMAT: "yyyy-MM-dd'T'HH:mm:ss",
        })
        mock_spark_session.read.format().options().load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_JDBC)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option.assert_called_once_with(constants.JDBC_TABLE, "table")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().option().option().mode().save.assert_called_once()

