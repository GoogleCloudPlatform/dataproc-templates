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

from dataproc_templates.jdbc.jdbc_to_gcs import JDBCToGCSTemplate
import dataproc_templates.util.template_constants as constants
import dataproc_templates.util.secret_manager as secret_manager


class TestJDBCToGCSTemplate:
    """
    Test suite for JDBCToGCSTemplate
    """

    def test_parse_args1(self):
        """Tests JDBCToGCSTemplate.parse_args()"""

        jdbc_to_gcs_template = JDBCToGCSTemplate()
        parsed_args = jdbc_to_gcs_template.parse_args(
            ["--jdbctogcs.input.url=url",
             "--jdbctogcs.input.driver=driver",
             "--jdbctogcs.input.table=table1",
             "--jdbctogcs.input.partitioncolumn=column",
             "--jdbctogcs.input.lowerbound=1",
             "--jdbctogcs.input.upperbound=2",
             "--jdbctogcs.numpartitions=5",
             "--jdbctogcs.input.fetchsize=100",
             "--jdbctogcs.input.sessioninitstatement=EXEC some_setup_sql('data1')",
             "--jdbctogcs.output.location=gs://test",
             "--jdbctogcs.output.format=csv",
             "--jdbctogcs.output.mode=append",
             "--jdbctogcs.output.partitioncolumn=column"
             ])

        assert parsed_args["jdbctogcs.input.url"] == "url"
        assert parsed_args["jdbctogcs.input.driver"] == "driver"
        assert parsed_args["jdbctogcs.input.table"] == "table1"
        assert parsed_args["jdbctogcs.input.partitioncolumn"] == "column"
        assert parsed_args["jdbctogcs.input.lowerbound"] == "1"
        assert parsed_args["jdbctogcs.input.upperbound"] == "2"
        assert parsed_args["jdbctogcs.numpartitions"] == "5"
        assert parsed_args["jdbctogcs.input.fetchsize"] == 100
        assert parsed_args["jdbctogcs.input.sessioninitstatement"] == "EXEC some_setup_sql('data1')"
        assert parsed_args["jdbctogcs.output.location"] == "gs://test"
        assert parsed_args["jdbctogcs.output.format"] == "csv"
        assert parsed_args["jdbctogcs.output.mode"] == "append"
        assert parsed_args["jdbctogcs.output.partitioncolumn"] == "column"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args2(self, mock_spark_session):
        """Tests JDBCToGCSTemplate write parquet"""

        jdbc_to_gcs_template = JDBCToGCSTemplate()

        mock_parsed_args = jdbc_to_gcs_template.parse_args(
            ["--jdbctogcs.input.url=url",
             "--jdbctogcs.input.driver=driver",
             "--jdbctogcs.input.table=table1",
             "--jdbctogcs.input.partitioncolumn=column",
             "--jdbctogcs.input.lowerbound=1",
             "--jdbctogcs.input.upperbound=2",
             "--jdbctogcs.numpartitions=5",
             "--jdbctogcs.input.sessioninitstatement=EXEC some_setup_sql('data2')",
             "--jdbctogcs.output.location=gs://test",
             "--jdbctogcs.output.format=parquet",
             "--jdbctogcs.output.mode=overwrite"
             ])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_JDBC)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.JDBC_URL: "url",
            constants.JDBC_DRIVER: "driver",
            constants.JDBC_TABLE: "table1",
            constants.JDBC_PARTITIONCOLUMN: "column",
            constants.JDBC_LOWERBOUND: "1",
            constants.JDBC_UPPERBOUND: "2",
            constants.JDBC_NUMPARTITIONS: "5",
            constants.JDBC_FETCHSIZE: 0,
            constants.JDBC_SESSIONINITSTATEMENT: "EXEC some_setup_sql('data2')"
        })
        mock_spark_session.read.format().options().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write.mode().parquet.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args3(self, mock_spark_session):
        """Tests JDBCToGCSTemplate write avro"""

        jdbc_to_gcs_template = JDBCToGCSTemplate()

        mock_parsed_args = jdbc_to_gcs_template.parse_args(
            ["--jdbctogcs.input.url=url",
             "--jdbctogcs.input.driver=driver",
             "--jdbctogcs.input.table=table1",
             "--jdbctogcs.input.partitioncolumn=column",
             "--jdbctogcs.input.lowerbound=1",
             "--jdbctogcs.input.upperbound=2",
             "--jdbctogcs.numpartitions=5",
             "--jdbctogcs.input.fetchsize=50",
             "--jdbctogcs.output.location=gs://test",
             "--jdbctogcs.output.format=avro",
             "--jdbctogcs.output.mode=append"
             ])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_JDBC)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.JDBC_URL: "url",
            constants.JDBC_DRIVER: "driver",
            constants.JDBC_TABLE: "table1",
            constants.JDBC_PARTITIONCOLUMN: "column",
            constants.JDBC_LOWERBOUND: "1",
            constants.JDBC_UPPERBOUND: "2",
            constants.JDBC_NUMPARTITIONS: "5",
            constants.JDBC_FETCHSIZE: 50
        })
        mock_spark_session.read.format().options().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().format.assert_called_once_with(constants.FORMAT_AVRO)
        mock_spark_session.dataframe.DataFrame.write.mode().format().save.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args4(self, mock_spark_session):
        """Tests JDBCToGCSTemplate write csv"""

        jdbc_to_gcs_template = JDBCToGCSTemplate()

        mock_parsed_args = jdbc_to_gcs_template.parse_args(
            ["--jdbctogcs.input.url=url",
             "--jdbctogcs.input.driver=driver",
             "--jdbctogcs.input.table=table1",
             "--jdbctogcs.input.partitioncolumn=column",
             "--jdbctogcs.input.lowerbound=1",
             "--jdbctogcs.input.upperbound=2",
             "--jdbctogcs.numpartitions=5",
             "--jdbctogcs.output.location=gs://test",
             "--jdbctogcs.output.format=csv",
             "--jdbctogcs.output.mode=ignore"
             ])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_JDBC)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.JDBC_URL: "url",
            constants.JDBC_DRIVER: "driver",
            constants.JDBC_TABLE: "table1",
            constants.JDBC_PARTITIONCOLUMN: "column",
            constants.JDBC_LOWERBOUND: "1",
            constants.JDBC_UPPERBOUND: "2",
            constants.JDBC_NUMPARTITIONS: "5",
            constants.JDBC_FETCHSIZE: 0
        })
        mock_spark_session.read.format().options().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_IGNORE)
        mock_spark_session.dataframe.DataFrame.write.mode().options.assert_called_once_with(**{constants.CSV_HEADER: 'true'})
        mock_spark_session.dataframe.DataFrame.write.mode().options().csv.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args5(self, mock_spark_session):
        """Tests JDBCToGCSTemplate write json"""

        jdbc_to_gcs_template = JDBCToGCSTemplate()

        mock_parsed_args = jdbc_to_gcs_template.parse_args(
            ["--jdbctogcs.input.url=url",
             "--jdbctogcs.input.driver=driver",
             "--jdbctogcs.input.table=table1",
             "--jdbctogcs.input.partitioncolumn=column",
             "--jdbctogcs.input.lowerbound=1",
             "--jdbctogcs.input.upperbound=2",
             "--jdbctogcs.numpartitions=5",
             "--jdbctogcs.output.location=gs://test",
             "--jdbctogcs.output.format=json",
             "--jdbctogcs.output.mode=ignore"
             ])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.JDBC_URL: "url",
            constants.JDBC_DRIVER: "driver",
            constants.JDBC_TABLE: "table1",
            constants.JDBC_PARTITIONCOLUMN: "column",
            constants.JDBC_LOWERBOUND: "1",
            constants.JDBC_UPPERBOUND: "2",
            constants.JDBC_NUMPARTITIONS: "5",
            constants.JDBC_FETCHSIZE: 0
        })
        mock_spark_session.read.format().options().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_IGNORE)
        #mock_spark_session.dataframe.DataFrame.write.mode().json.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args6(self, mock_spark_session):
        """Tests JDBCToGCSTemplate pass args"""

        jdbc_to_gcs_template = JDBCToGCSTemplate()

        mock_parsed_args = jdbc_to_gcs_template.parse_args(
            ["--jdbctogcs.input.url=url",
             "--jdbctogcs.input.driver=driver",
             "--jdbctogcs.input.table=table1",
             "--jdbctogcs.output.location=gs://test",
             "--jdbctogcs.output.format=csv",
             "--jdbctogcs.output.mode=append"
             ])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.JDBC_URL: "url",
            constants.JDBC_DRIVER: "driver",
            constants.JDBC_TABLE: "table1",
            constants.JDBC_NUMPARTITIONS: "10",
            constants.JDBC_FETCHSIZE: 0
        })
        mock_spark_session.read.format().options().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().options.assert_called_once_with(**{constants.CSV_HEADER: 'true'})
        mock_spark_session.dataframe.DataFrame.write.mode().options().csv.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args7(self, mock_spark_session):
        """Tests JDBCToGCSTemplate pass args"""

        jdbc_to_gcs_template = JDBCToGCSTemplate()

        mock_parsed_args = jdbc_to_gcs_template.parse_args(
            ["--jdbctogcs.input.url=url",
             "--jdbctogcs.input.driver=driver",
             "--jdbctogcs.input.table=table1",
             "--jdbctogcs.output.location=gs://test",
             "--jdbctogcs.output.format=csv",
             "--jdbctogcs.output.mode=append",
             "--jdbctogcs.output.partitioncolumn=column"
             ])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_JDBC)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.JDBC_URL: "url",
            constants.JDBC_DRIVER: "driver",
            constants.JDBC_TABLE: "table1",
            constants.JDBC_NUMPARTITIONS: "10",
            constants.JDBC_FETCHSIZE: 0
        })
        mock_spark_session.read.format().options().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy.assert_called_once_with("column")
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy().options.assert_called_once_with(**{constants.CSV_HEADER: 'true'})
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy().options().csv.assert_called_once_with("gs://test")


    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args8(self, mock_spark_session):
        """Tests JDBCToGCSTemplate pass args with secret"""

        jdbc_to_gcs_template = JDBCToGCSTemplate()

        mock_parsed_args = jdbc_to_gcs_template.parse_args(
            ["--jdbctogcs.input.url.secret=jdbctobqconn",
             "--jdbctogcs.input.driver=driver",
             "--jdbctogcs.input.table=table1",
             "--jdbctogcs.output.location=gs://test",
             "--jdbctogcs.output.format=csv",
             "--jdbctogcs.output.mode=append",
             "--jdbctogcs.output.partitioncolumn=column"
             ])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_JDBC)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.JDBC_URL: secret_manager.access_secret_version("jdbctobqconn"),
            constants.JDBC_DRIVER: "driver",
            constants.JDBC_TABLE: "table1",
            constants.JDBC_NUMPARTITIONS: "10",
            constants.JDBC_FETCHSIZE: 0
        })
        mock_spark_session.read.format().options().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy.assert_called_once_with("column")
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy().options.assert_called_once_with(**{constants.CSV_HEADER: 'true'})
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy().options().csv.assert_called_once_with("gs://test")

