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
             "--jdbctogcs.output.location=gs://test",
             "--jdbctogcs.output.format=parquet",
             "--jdbctogcs.output.mode=overwrite"
             ])
        mock_spark_session.read.format().option().option().option().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.JDBC_PARTITIONCOLUMN, "column")
        mock_spark_session.read.format().option().option().option().option().option.assert_called_with(constants.JDBC_LOWERBOUND, "1")
        mock_spark_session.read.format().option().option().option().option().option().option.assert_called_with(constants.JDBC_UPPERBOUND, "2")
        mock_spark_session.read.format().option().option().option().option().option().option().option.assert_called_with(constants.JDBC_NUMPARTITIONS, "5")
        mock_spark_session.read.format().option().option().option().option().option().option().option().load()
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
             "--jdbctogcs.output.location=gs://test",
             "--jdbctogcs.output.format=avro",
             "--jdbctogcs.output.mode=append"
             ])
        mock_spark_session.read.format().option().option().option().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.JDBC_PARTITIONCOLUMN, "column")
        mock_spark_session.read.format().option().option().option().option().option.assert_called_with(constants.JDBC_LOWERBOUND, "1")
        mock_spark_session.read.format().option().option().option().option().option().option.assert_called_with(constants.JDBC_UPPERBOUND, "2")
        mock_spark_session.read.format().option().option().option().option().option().option().option.assert_called_with(constants.JDBC_NUMPARTITIONS, "5")
        mock_spark_session.read.format().option().option().option().option().option().option().option().load()
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
        mock_spark_session.read.format().option().option().option().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.JDBC_PARTITIONCOLUMN, "column")
        mock_spark_session.read.format().option().option().option().option().option.assert_called_with(constants.JDBC_LOWERBOUND, "1")
        mock_spark_session.read.format().option().option().option().option().option().option.assert_called_with(constants.JDBC_UPPERBOUND, "2")
        mock_spark_session.read.format().option().option().option().option().option().option().option.assert_called_with(constants.JDBC_NUMPARTITIONS, "5")
        mock_spark_session.read.format().option().option().option().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_IGNORE)
        mock_spark_session.dataframe.DataFrame.write.mode().option.assert_called_once_with(constants.HEADER, True)
        mock_spark_session.dataframe.DataFrame.write.mode().option().csv.assert_called_once_with("gs://test")
        
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
        mock_spark_session.read.format().option().option().option().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.JDBC_PARTITIONCOLUMN, "column")
        mock_spark_session.read.format().option().option().option().option().option.assert_called_with(constants.JDBC_LOWERBOUND, "1")
        mock_spark_session.read.format().option().option().option().option().option().option.assert_called_with(constants.JDBC_UPPERBOUND, "2")
        mock_spark_session.read.format().option().option().option().option().option().option().option.assert_called_with(constants.JDBC_NUMPARTITIONS, "5")
        mock_spark_session.read.format().option().option().option().option().option().option().option().load()
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
        mock_spark_session.read.format().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.JDBC_NUMPARTITIONS, "10")
        mock_spark_session.read.format().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().option.assert_called_once_with(constants.HEADER, True)
        mock_spark_session.dataframe.DataFrame.write.mode().option().csv.assert_called_once_with("gs://test")
        
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
        mock_spark_session.read.format().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.JDBC_NUMPARTITIONS, "10")
        mock_spark_session.read.format().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy.assert_called_once_with("column")
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy().option.assert_called_once_with(constants.HEADER, True)
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy().option().csv.assert_called_once_with("gs://test")
