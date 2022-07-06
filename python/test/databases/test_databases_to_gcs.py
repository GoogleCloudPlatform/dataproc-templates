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

from dataproc_templates.databases.databases_to_gcs import DatabasesToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestDatabasesToGCSTemplate:
    """
    Test suite for DatabasesToGCSTemplate
    """

    def test_parse_args(self):
        """Tests DatabasesToGCSTemplate.parse_args()"""

        db_to_gcs_template = DatabasesToGCSTemplate()
        parsed_args = db_to_gcs_template.parse_args(
            ["--db.gcs.source.jdbc.url=jdbc:database://localhost:port/db_name",
             "--db.gcs.jdbc.driver=my.jdbc.driver",
             "--db.gcs.source.table=table",
             "--db.gcs.destination.location=gs://test",
             "--db.gcs.output.format=parquet",
             "--db.gcs.output.mode=overwrite"])

        assert parsed_args["db.gcs.source.jdbc.url"] == "jdbc:database://localhost:port/db_name"
        assert parsed_args["db.gcs.jdbc.driver"] == "my.jdbc.driver"
        assert parsed_args["db.gcs.source.table"] == "table"
        assert parsed_args["db.gcs.destination.location"] == "gs://test"
        assert parsed_args["db.gcs.output.format"] == "parquet"
        assert parsed_args["db.gcs.output.mode"] == "overwrite"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_parquet(self, mock_spark_session):
        """Tests DatabasesToGCSTemplate runs for parquet format output"""

        db_to_gcs_template = DatabasesToGCSTemplate()
        mock_parsed_args = db_to_gcs_template.parse_args(
            ["--db.gcs.source.jdbc.url=jdbc:database://localhost:port/db_name",
             "--db.gcs.jdbc.driver=my.jdbc.driver",
             "--db.gcs.source.table=table",
             "--db.gcs.destination.location=gs://test",
             "--db.gcs.output.format=parquet",
             "--db.gcs.output.mode=overwrite"])
        
        mock_spark_session.read.format().option().option().option(
        ).load.return_value = mock_spark_session.dataframe.DataFrame
        db_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        
        mock_spark_session.read \
            .format.assert_called_with("jdbc")
        mock_spark_session.read \
            .format() \
            .option.assert_called_with(constants.JDBC_URL,"jdbc:database://localhost:port/db_name")
        mock_spark_session.read \
            .format() \
            .option().option().option.assert_called_with(constants.JDBC_TABLE,"table")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option() \
            .load.assert_called_with()
            
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .parquet.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_avro(self, mock_spark_session):
        """Tests DatabasesToGCSTemplate runs for avro format output"""

        db_to_gcs_template = DatabasesToGCSTemplate()
        mock_parsed_args = db_to_gcs_template.parse_args(
            ["--db.gcs.source.jdbc.url=jdbc:database://localhost:port/db_name",
             "--db.gcs.jdbc.driver=my.jdbc.driver",
             "--db.gcs.source.table=table",
             "--db.gcs.destination.location=gs://test",
             "--db.gcs.output.format=avro",
             "--db.gcs.output.mode=overwrite"])
        mock_spark_session.read.format().option().option().option(
        ).load.return_value = mock_spark_session.dataframe.DataFrame
        db_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        
        mock_spark_session.read \
            .format.assert_called_with("jdbc")
        mock_spark_session.read \
            .format() \
            .option.assert_called_with(constants.JDBC_URL,"jdbc:database://localhost:port/db_name")
        mock_spark_session.read \
            .format() \
            .option().option().option.assert_called_with(constants.JDBC_TABLE,"table")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option() \
            .load.assert_called_with()
            
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .format.assert_called_once_with(constants.FORMAT_AVRO)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .format() \
            .save.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_csv(self, mock_spark_session):
        """Tests DatabasesToGCSTemplate runs for csv format output"""

        db_to_gcs_template = DatabasesToGCSTemplate()
        mock_parsed_args = db_to_gcs_template.parse_args(
            ["--db.gcs.source.jdbc.url=jdbc:database://localhost:port/db_name",
             "--db.gcs.source.table=table",
             "--db.gcs.jdbc.driver=my.jdbc.driver",
             "--db.gcs.destination.location=gs://test",
             "--db.gcs.output.format=csv",
             "--db.gcs.output.mode=overwrite"])
        mock_spark_session.read.format().option().option().option(
        ).load.return_value = mock_spark_session.dataframe.DataFrame
        db_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        
        mock_spark_session.read \
            .format.assert_called_with("jdbc")
        mock_spark_session.read \
            .format() \
            .option.assert_called_with(constants.JDBC_URL,"jdbc:database://localhost:port/db_name")
        mock_spark_session.read \
            .format() \
            .option().option().option.assert_called_with(constants.JDBC_TABLE,"table")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option() \
            .load.assert_called_with()
            
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .option.assert_called_once_with(constants.HEADER, True)
            
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .option() \
            .csv.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_json(self, mock_spark_session):
        """Tests DatabasesToGCSTemplate runs with json format output"""

        db_to_gcs_template = DatabasesToGCSTemplate()
        mock_parsed_args = db_to_gcs_template.parse_args(
            ["--db.gcs.source.jdbc.url=jdbc:database://localhost:port/db_name",
             "--db.gcs.jdbc.driver=my.jdbc.driver",
             "--db.gcs.source.table=table",
             "--db.gcs.destination.location=gs://test",
             "--db.gcs.output.format=json",
             "--db.gcs.output.mode=overwrite"])
        mock_spark_session.read.format().option().option().option(
        ).load.return_value = mock_spark_session.dataframe.DataFrame
        db_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        
        mock_spark_session.read \
            .format.assert_called_with("jdbc")
        mock_spark_session.read \
            .format() \
            .option.assert_called_with(constants.JDBC_URL,"jdbc:database://localhost:port/db_name")
        mock_spark_session.read \
            .format() \
            .option().option().option.assert_called_with(constants.JDBC_TABLE,"table")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option() \
            .load.assert_called_with()
            
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .json.assert_called_once_with("gs://test")
