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

import logging
import mock
import pyspark

from dataproc_templates.snowflake.snowflake_to_gcs import SnowflakeToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestSnowflakeToGCSTemplate:
    """
    Test suite for SnowflakeToGCSTemplate
    """

    def test_parse_args_query(self):
        """Tests SnowflakeToGCSTemplate.parse_args()"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()
        parsed_args = snowflake_to_gcs_template.parse_args(
            ["--snowflake.to.gcs.sf.url=url",
             "--snowflake.to.gcs.sf.user=user",
             "--snowflake.to.gcs.sf.password=password",
             "--snowflake.to.gcs.sf.query=select 1",
             "--snowflake.to.gcs.output.location=gs://test",
             "--snowflake.to.gcs.output.format=csv",
             "--snowflake.to.gcs.partition.column=col"
             ])

        assert parsed_args["snowflake.to.gcs.sf.url"] == "url"
        assert parsed_args["snowflake.to.gcs.sf.user"] == "user"
        assert parsed_args["snowflake.to.gcs.sf.password"] == "password"
        assert parsed_args["snowflake.to.gcs.sf.query"] == "select 1"
        assert parsed_args["snowflake.to.gcs.output.location"] == "gs://test"
        assert parsed_args["snowflake.to.gcs.output.format"] == "csv"
        assert parsed_args["snowflake.to.gcs.partition.column"] == "col"

    def test_parse_args_table(self):
        """Tests SnowflakeToGCSTemplate.parse_args()"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()
        parsed_args = snowflake_to_gcs_template.parse_args(
            ["--snowflake.to.gcs.sf.url=url",
             "--snowflake.to.gcs.sf.user=user",
             "--snowflake.to.gcs.sf.password=password",
             "--snowflake.to.gcs.sf.database=database",
             "--snowflake.to.gcs.sf.schema=schema",
             "--snowflake.to.gcs.sf.table=table",
             "--snowflake.to.gcs.output.location=gs://test",
             "--snowflake.to.gcs.output.format=csv",
             "--snowflake.to.gcs.partition.column=col"
             ])

        assert parsed_args["snowflake.to.gcs.sf.url"] == "url"
        assert parsed_args["snowflake.to.gcs.sf.user"] == "user"
        assert parsed_args["snowflake.to.gcs.sf.password"] == "password"
        assert parsed_args["snowflake.to.gcs.sf.database"] == "database"
        assert parsed_args["snowflake.to.gcs.sf.schema"] == "schema"
        assert parsed_args["snowflake.to.gcs.sf.table"] == "table"
        assert parsed_args["snowflake.to.gcs.output.location"] == "gs://test"
        assert parsed_args["snowflake.to.gcs.output.format"] == "csv"
        assert parsed_args["snowflake.to.gcs.partition.column"] == "col"

    def test_parse_args_warehouse(self):
        """Tests SnowflakeToGCSTemplate.parse_args()"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()
        parsed_args = snowflake_to_gcs_template.parse_args(
            ["--snowflake.to.gcs.sf.url=url",
             "--snowflake.to.gcs.sf.user=user",
             "--snowflake.to.gcs.sf.password=password",
             "--snowflake.to.gcs.sf.database=database",
             "--snowflake.to.gcs.sf.warehouse=dwh",
             "--snowflake.to.gcs.sf.schema=schema",
             "--snowflake.to.gcs.sf.table=table",
             "--snowflake.to.gcs.output.location=gs://test",
             "--snowflake.to.gcs.output.format=csv",
             "--snowflake.to.gcs.partition.column=col",
             "--snowflake.to.gcs.sf.autopushdown=no",
             "--snowflake.to.gcs.output.mode=append"
             ])

        assert parsed_args["snowflake.to.gcs.sf.url"] == "url"
        assert parsed_args["snowflake.to.gcs.sf.user"] == "user"
        assert parsed_args["snowflake.to.gcs.sf.password"] == "password"
        assert parsed_args["snowflake.to.gcs.sf.database"] == "database"
        assert parsed_args["snowflake.to.gcs.sf.warehouse"] == "dwh"
        assert parsed_args["snowflake.to.gcs.sf.schema"] == "schema"
        assert parsed_args["snowflake.to.gcs.sf.table"] == "table"
        assert parsed_args["snowflake.to.gcs.output.location"] == "gs://test"
        assert parsed_args["snowflake.to.gcs.output.format"] == "csv"
        assert parsed_args["snowflake.to.gcs.partition.column"] == "col"
        assert parsed_args["snowflake.to.gcs.sf.autopushdown"] == "no"
        assert parsed_args["snowflake.to.gcs.output.mode"] == "append"

    @mock.patch.object(logging, 'Logger')
    def test_get_read_options_table(self, Logger):
        """Tests SnowflakeToGCSTemplate to get read options"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()

        mock_parsed_args = snowflake_to_gcs_template.parse_args(
            ["--snowflake.to.gcs.sf.url=url",
             "--snowflake.to.gcs.sf.user=user",
             "--snowflake.to.gcs.sf.password=password",
             "--snowflake.to.gcs.sf.database=database",
             "--snowflake.to.gcs.sf.warehouse=dwh",
             "--snowflake.to.gcs.sf.schema=schema",
             "--snowflake.to.gcs.sf.table=table",
             "--snowflake.to.gcs.output.location=gs://test",
             "--snowflake.to.gcs.output.format=csv",
             "--snowflake.to.gcs.partition.column=col",
             "--snowflake.to.gcs.sf.autopushdown=no",
             "--snowflake.to.gcs.output.mode=append"
             ]
        )

        sf_options, table_options = snowflake_to_gcs_template.get_read_options(Logger, mock_parsed_args)

        assert sf_options["sfURL"] == "url"
        assert sf_options["sfUser"] == "user"
        assert sf_options["sfPassword"] == "password"
        assert sf_options["sfDatabase"] == "database"
        assert sf_options["sfSchema"] == "schema"
        assert sf_options["sfWarehouse"] == "dwh"
        assert sf_options["autopushdown"] == "no"

        assert table_options["dbtable"] == "table"
        assert table_options["query"] == ""

    @mock.patch.object(logging, 'Logger')
    def test_get_read_options_query(self, Logger):
        """Tests SnowflakeToGCSTemplate to get read options"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()

        mock_parsed_args = snowflake_to_gcs_template.parse_args(
            ["--snowflake.to.gcs.sf.url=url",
             "--snowflake.to.gcs.sf.user=user",
             "--snowflake.to.gcs.sf.password=password",
             "--snowflake.to.gcs.sf.warehouse=dwh",
             "--snowflake.to.gcs.sf.query=select 1",
             "--snowflake.to.gcs.output.location=gs://test",
             "--snowflake.to.gcs.output.format=csv",
             "--snowflake.to.gcs.partition.column=col",
             "--snowflake.to.gcs.sf.autopushdown=yes",
             "--snowflake.to.gcs.output.mode=append"
             ]
        )

        sf_options, table_options = snowflake_to_gcs_template.get_read_options(Logger, mock_parsed_args)

        assert sf_options["sfURL"] == "url"
        assert sf_options["sfUser"] == "user"
        assert sf_options["sfPassword"] == "password"
        assert sf_options["sfDatabase"] == ""
        assert sf_options["sfSchema"] == ""
        assert sf_options["sfWarehouse"] == "dwh"
        assert sf_options["autopushdown"] == "yes"

        assert table_options["dbtable"] == ""
        assert table_options["query"] == "select 1"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch.object(logging, 'Logger')
    def test_read_data_1(self, Logger, mock_spark_session):
        """Tests SnowflakeToGCSTemplate to read data"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()

        sf_opt = {
            "sfURL" : "url",
            "sfUser" : "user",
            "sfPassword" : "password",
            "sfDatabase" : "database",
            "sfSchema" : "schema",
            "sfWarehouse" : "dwh",
            "autopushdown" : "yes"
        }

        data_opt = {
            "dbtable" : "table",
            "query" : ""
        }

        snowflake_to_gcs_template.read_data(Logger,mock_spark_session,sf_opt,data_opt)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_SNOWFLAKE)
        mock_spark_session.read.format().options.assert_called_with(**sf_opt)
        mock_spark_session.read.format().options().option.assert_called_with("dbtable",data_opt["dbtable"])
        mock_spark_session.read.format().options().option().load.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch.object(logging, 'Logger')
    def test_read_data_2(self, Logger, mock_spark_session):
        """Tests SnowflakeToGCSTemplate to read data"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()

        sf_opt = {
            "sfURL" : "url",
            "sfUser" : "user",
            "sfPassword" : "password",
            "sfDatabase" : "",
            "sfSchema" : "",
            "sfWarehouse" : "dwh",
            "autopushdown" : "yes"
        }

        data_opt = {
            "dbtable" : "",
            "query" : "select 1"
        }

        snowflake_to_gcs_template.read_data(Logger,mock_spark_session,sf_opt,data_opt)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_SNOWFLAKE)
        mock_spark_session.read.format().options.assert_called_with(**sf_opt)
        mock_spark_session.read.format().options().option.assert_called_with("query",data_opt["query"])
        mock_spark_session.read.format().options().option().load.assert_called_once()


    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch.object(logging, 'Logger')
    def test_write_data_csv(self, Logger, mock_spark_session):
        """Tests SnowflakeToGCSTemplate to write data"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()

        mock_parsed_args = snowflake_to_gcs_template.parse_args(
            ["--snowflake.to.gcs.sf.url=url",
             "--snowflake.to.gcs.sf.user=user",
             "--snowflake.to.gcs.sf.password=password",
             "--snowflake.to.gcs.sf.warehouse=dwh",
             "--snowflake.to.gcs.sf.query=select 1",
             "--snowflake.to.gcs.output.location=gs://test",
             "--snowflake.to.gcs.output.format=csv",
             "--snowflake.to.gcs.partition.column=col",
             "--snowflake.to.gcs.sf.autopushdown=yes",
             "--snowflake.to.gcs.output.mode=append"
             ]
        )

        mock_spark_session.read.format().options().option().load.return_value = mock_spark_session.dataframe.DataFrame
        snowflake_to_gcs_template.write_data(Logger, mock_parsed_args, mock_spark_session.dataframe.DataFrame)
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy.assert_called_with("col")
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy().options.assert_called_with(**{constants.CSV_HEADER: 'true'})
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy().options().csv.assert_called_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch.object(logging, 'Logger')
    def test_write_data_parquet(self, Logger, mock_spark_session):
        """Tests SnowflakeToGCSTemplate to write data"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()

        mock_parsed_args = snowflake_to_gcs_template.parse_args(
            ["--snowflake.to.gcs.sf.url=url",
             "--snowflake.to.gcs.sf.user=user",
             "--snowflake.to.gcs.sf.password=password",
             "--snowflake.to.gcs.sf.warehouse=dwh",
             "--snowflake.to.gcs.sf.query=select 1",
             "--snowflake.to.gcs.output.location=gs://test",
             "--snowflake.to.gcs.output.format=parquet",
             "--snowflake.to.gcs.sf.autopushdown=yes",
             "--snowflake.to.gcs.output.mode=append"
             ]
        )

        mock_spark_session.read.format().options().option().load.return_value = mock_spark_session.dataframe.DataFrame
        snowflake_to_gcs_template.write_data(Logger, mock_parsed_args, mock_spark_session.dataframe.DataFrame)
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().parquet.assert_called_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch.object(logging, 'Logger')
    def test_write_data_json(self, Logger, mock_spark_session):
        """Tests SnowflakeToGCSTemplate to write data"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()

        mock_parsed_args = snowflake_to_gcs_template.parse_args(
            ["--snowflake.to.gcs.sf.url=url",
             "--snowflake.to.gcs.sf.user=user",
             "--snowflake.to.gcs.sf.password=password",
             "--snowflake.to.gcs.sf.warehouse=dwh",
             "--snowflake.to.gcs.sf.query=select 1",
             "--snowflake.to.gcs.output.location=gs://test",
             "--snowflake.to.gcs.output.format=json",
             "--snowflake.to.gcs.sf.autopushdown=yes",
             "--snowflake.to.gcs.output.mode=append"
             ]
        )

        mock_spark_session.read.format().options().option().load.return_value = mock_spark_session.dataframe.DataFrame
        snowflake_to_gcs_template.write_data(Logger, mock_parsed_args, mock_spark_session.dataframe.DataFrame)
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().json.assert_called_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch.object(logging, 'Logger')
    def test_write_data_avro(self, Logger, mock_spark_session):
        """Tests SnowflakeToGCSTemplate to write data"""

        snowflake_to_gcs_template = SnowflakeToGCSTemplate()

        mock_parsed_args = snowflake_to_gcs_template.parse_args(
            ["--snowflake.to.gcs.sf.url=url",
             "--snowflake.to.gcs.sf.user=user",
             "--snowflake.to.gcs.sf.password=password",
             "--snowflake.to.gcs.sf.warehouse=dwh",
             "--snowflake.to.gcs.sf.query=select 1",
             "--snowflake.to.gcs.output.location=gs://test",
             "--snowflake.to.gcs.output.format=avro",
             "--snowflake.to.gcs.sf.autopushdown=yes",
             "--snowflake.to.gcs.output.mode=append"
             ]
        )

        mock_spark_session.read.format().options().option().load.return_value = mock_spark_session.dataframe.DataFrame
        snowflake_to_gcs_template.write_data(Logger, mock_parsed_args, mock_spark_session.dataframe.DataFrame)
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().format.assert_called_with(constants.FORMAT_AVRO)
        mock_spark_session.dataframe.DataFrame.write.mode().format().save.assert_called_with("gs://test")
