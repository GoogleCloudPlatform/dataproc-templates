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

from dataproc_templates.jdbc.jdbc_to_jdbc import JDBCToJDBCTemplate
import dataproc_templates.util.template_constants as constants


class TestJDBCToJDBCTemplate:
    """
    Test suite for JDBCToJDBCTemplate
    """

    def test_parse_args(self):
        """Tests JDBCToJDBCTemplate.parse_args()"""

        jdbc_to_jdbc_template = JDBCToJDBCTemplate()
        parsed_args = jdbc_to_jdbc_template.parse_args(
            ["--jdbctojdbc.input.url=url",
             "--jdbctojdbc.input.driver=driver",
             "--jdbctojdbc.input.table=table1",
             "--jdbctojdbc.input.partitioncolumn=column",
             "--jdbctojdbc.input.lowerbound=1",
             "--jdbctojdbc.input.upperbound=2",
             "--jdbctojdbc.numpartitions=5",
             "--jdbctojdbc.output.url=url",
             "--jdbctojdbc.output.driver=driver",
             "--jdbctojdbc.output.table=table2",
             "--jdbctojdbc.output.create_table.option=dummy",
             "--jdbctojdbc.output.mode=append",
             "--jdbctojdbc.output.batch.size=1000"
             ])       
        
        assert parsed_args["jdbctojdbc.input.url"] == "url"
        assert parsed_args["jdbctojdbc.input.driver"] == "driver"
        assert parsed_args["jdbctojdbc.input.table"] == "table1"
        assert parsed_args["jdbctojdbc.input.partitioncolumn"] == "column"
        assert parsed_args["jdbctojdbc.input.lowerbound"] == "1"
        assert parsed_args["jdbctojdbc.input.upperbound"] == "2"
        assert parsed_args["jdbctojdbc.numpartitions"] == "5"
        assert parsed_args["jdbctojdbc.output.url"] == "url"
        assert parsed_args["jdbctojdbc.output.driver"] == "driver"
        assert parsed_args["jdbctojdbc.output.table"] == "table2"
        assert parsed_args["jdbctojdbc.output.create_table.option"] == "dummy"
        assert parsed_args["jdbctojdbc.output.mode"] == "append"
        assert parsed_args["jdbctojdbc.output.batch.size"] == "1000"
        

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args1(self, mock_spark_session):
        """Tests JDBCToJDBCTemplate pass args"""

        jdbc_to_jdbc_template = JDBCToJDBCTemplate()

        mock_parsed_args = jdbc_to_jdbc_template.parse_args(
            ["--jdbctojdbc.input.url=url",
             "--jdbctojdbc.input.driver=driver",
             "--jdbctojdbc.input.table=table1",
             "--jdbctojdbc.input.partitioncolumn=column",
             "--jdbctojdbc.input.lowerbound=1",
             "--jdbctojdbc.input.upperbound=2",
             "--jdbctojdbc.numpartitions=5",
             "--jdbctojdbc.output.url=url",
             "--jdbctojdbc.output.driver=driver",
             "--jdbctojdbc.output.table=table2",
             "--jdbctojdbc.output.create_table.option=dummy",
             "--jdbctojdbc.output.mode=append",
             "--jdbctojdbc.output.batch.size=1000"
             ])
        mock_spark_session.read.format().option().option().option().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_jdbc_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.JDBC_PARTITIONCOLUMN, "column")
        mock_spark_session.read.format().option().option().option().option().option.assert_called_with(constants.JDBC_LOWERBOUND, "1")
        mock_spark_session.read.format().option().option().option().option().option().option.assert_called_with(constants.JDBC_UPPERBOUND, "2")
        mock_spark_session.read.format().option().option().option().option().option().option().option.assert_called_with(constants.JDBC_NUMPARTITIONS, "5")
        mock_spark_session.read.format().option().option().option().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(constants.FORMAT_JDBC)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_called_once_with(constants.JDBC_URL, "url")
        mock_spark_session.dataframe.DataFrame.write.format().option().option.assert_called_once_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option.assert_called_once_with(constants.JDBC_TABLE, "table2")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option.assert_called_once_with(constants.JDBC_CREATE_TABLE_OPTIONS, "dummy")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option.assert_called_once_with(constants.JDBC_BATCH_SIZE, "1000")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option().option.assert_called_once_with(constants.JDBC_NUMPARTITIONS, "5")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option().option().mode().save.assert_called_once()


    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args2(self, mock_spark_session):
        """Tests JDBCToJDBCTemplate pass args"""

        jdbc_to_jdbc_template = JDBCToJDBCTemplate()

        mock_parsed_args = jdbc_to_jdbc_template.parse_args(
            ["--jdbctojdbc.input.url=url",
             "--jdbctojdbc.input.driver=driver",
             "--jdbctojdbc.input.table=table1",
             "--jdbctojdbc.input.partitioncolumn=column",
             "--jdbctojdbc.input.lowerbound=1",
             "--jdbctojdbc.input.upperbound=2",
             "--jdbctojdbc.numpartitions=5",
             "--jdbctojdbc.output.url=url",
             "--jdbctojdbc.output.driver=driver",
             "--jdbctojdbc.output.table=table2"
             ])
        mock_spark_session.read.format().option().option().option().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_jdbc_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.JDBC_PARTITIONCOLUMN, "column")
        mock_spark_session.read.format().option().option().option().option().option.assert_called_with(constants.JDBC_LOWERBOUND, "1")
        mock_spark_session.read.format().option().option().option().option().option().option.assert_called_with(constants.JDBC_UPPERBOUND, "2")
        mock_spark_session.read.format().option().option().option().option().option().option().option.assert_called_with(constants.JDBC_NUMPARTITIONS, "5")
        mock_spark_session.read.format().option().option().option().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(constants.FORMAT_JDBC)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_called_once_with(constants.JDBC_URL, "url")
        mock_spark_session.dataframe.DataFrame.write.format().option().option.assert_called_once_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option.assert_called_once_with(constants.JDBC_TABLE, "table2")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option.assert_called_once_with(constants.JDBC_CREATE_TABLE_OPTIONS, "")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option.assert_called_once_with(constants.JDBC_BATCH_SIZE, "1000")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option().option.assert_called_once_with(constants.JDBC_NUMPARTITIONS, "5")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option().option().mode().save.assert_called_once()
             
    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args3(self, mock_spark_session):
        """Tests JDBCToJDBCTemplate pass args"""

        jdbc_to_jdbc_template = JDBCToJDBCTemplate()

        mock_parsed_args = jdbc_to_jdbc_template.parse_args(
            ["--jdbctojdbc.input.url=url",
             "--jdbctojdbc.input.driver=driver",
             "--jdbctojdbc.input.table=table1",
             "--jdbctojdbc.output.url=url",
             "--jdbctojdbc.output.driver=driver",
             "--jdbctojdbc.output.table=table2"
             ])
        mock_spark_session.read.format().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        jdbc_to_jdbc_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_JDBC)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.JDBC_NUMPARTITIONS, "10")
        mock_spark_session.read.format().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(constants.FORMAT_JDBC)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_called_once_with(constants.JDBC_URL, "url")
        mock_spark_session.dataframe.DataFrame.write.format().option().option.assert_called_once_with(constants.JDBC_DRIVER, "driver")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option.assert_called_once_with(constants.JDBC_TABLE, "table2")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option.assert_called_once_with(constants.JDBC_CREATE_TABLE_OPTIONS, "")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option.assert_called_once_with(constants.JDBC_BATCH_SIZE, "1000")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option().option.assert_called_once_with(constants.JDBC_NUMPARTITIONS, "10")
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format().option().option().option().option().option().option().mode().save.assert_called_once()