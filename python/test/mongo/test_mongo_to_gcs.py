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

from dataproc_templates.mongo.mongo_to_gcs import MongoToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestMongoToGCSTemplate:
    """
    Test suite for MongoToGCSTemplate
    """

    def test_parse_args(self):
        """Tests MongoToGCSTemplate.parse_args()"""

        mongo_to_gcs_template = MongoToGCSTemplate()
        parsed_args = mongo_to_gcs_template.parse_args(
            ["--mongo.gcs.input.uri=mongodb://host:port",
             "--mongo.gcs.input.database=database",
             "--mongo.gcs.input.collection=collection",
             "--mongo.gcs.output.format=parquet",
             "--mongo.gcs.output.mode=overwrite",
             "--mongo.gcs.output.location=gs://test"])

        assert parsed_args["mongo.gcs.input.uri"] == "mongodb://host:port"
        assert parsed_args["mongo.gcs.input.database"] == "database"
        assert parsed_args["mongo.gcs.input.collection"] == "collection"
        assert parsed_args["mongo.gcs.output.format"] == "parquet"
        assert parsed_args["mongo.gcs.output.mode"] == "overwrite"
        assert parsed_args["mongo.gcs.output.location"] == "gs://test"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_parquet(self, mock_spark_session):
        """Tests MongoToGCSTemplate runs for parquet format output"""

        mongo_to_gcs_template = MongoToGCSTemplate()
        mock_parsed_args = mongo_to_gcs_template.parse_args(
            ["--mongo.gcs.input.uri=mongodb://host:port",
             "--mongo.gcs.input.database=database",
             "--mongo.gcs.input.collection=collection",
             "--mongo.gcs.output.format=parquet",
             "--mongo.gcs.output.mode=overwrite",
             "--mongo.gcs.output.location=gs://test"])
        mock_spark_session.read.format().option().option().option().load.return_value \
            = mock_spark_session.dataframe.DataFrame
        mongo_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format.assert_called_with(constants.FORMAT_MONGO)
        mock_spark_session.read \
            .format() \
            .option() \
            .option.assert_called_with(constants.MONGO_DATABASE,"database")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option.assert_called_with(constants.MONGO_COLLECTION,"collection")
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
        """Tests MongoToGCSTemplate runs for avro format output"""

        mongo_to_gcs_template = MongoToGCSTemplate()
        mock_parsed_args = mongo_to_gcs_template.parse_args(
            ["--mongo.gcs.input.uri=mongodb://host:port",
             "--mongo.gcs.input.database=database",
             "--mongo.gcs.input.collection=collection",
             "--mongo.gcs.output.format=avro",
             "--mongo.gcs.output.mode=append",
             "--mongo.gcs.output.location=gs://test"])
        mock_spark_session.read.format().option().option().option().load.return_value \
            = mock_spark_session.dataframe.DataFrame
        mongo_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format.assert_called_with(constants.FORMAT_MONGO)
        mock_spark_session.read \
            .format() \
            .option() \
            .option.assert_called_with(constants.MONGO_DATABASE,"database")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option.assert_called_with(constants.MONGO_COLLECTION,"collection")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option() \
            .load.assert_called_with()
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
        """Tests MongoToGCSTemplate runs for csv format output"""

        mongo_to_gcs_template = MongoToGCSTemplate()
        mock_parsed_args = mongo_to_gcs_template.parse_args(
            ["--mongo.gcs.input.uri=mongodb://host:port",
             "--mongo.gcs.input.database=database",
             "--mongo.gcs.input.collection=collection",
             "--mongo.gcs.output.format=csv",
             "--mongo.gcs.output.mode=ignore",
             "--mongo.gcs.output.location=gs://test"])
        mock_spark_session.read.format().option().option().option().load.return_value \
            = mock_spark_session.dataframe.DataFrame
        mongo_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format.assert_called_with(constants.FORMAT_MONGO)
        mock_spark_session.read \
            .format() \
            .option() \
            .option.assert_called_with(constants.MONGO_DATABASE,"database")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option.assert_called_with(constants.MONGO_COLLECTION,"collection")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option() \
            .load.assert_called_with()
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
        """Tests MongoToGCSTemplate runs for json format output"""

        mongo_to_gcs_template = MongoToGCSTemplate()
        mock_parsed_args = mongo_to_gcs_template.parse_args(
            ["--mongo.gcs.input.uri=mongodb://host:port",
             "--mongo.gcs.input.database=database",
             "--mongo.gcs.input.collection=collection",
             "--mongo.gcs.output.format=json",
             "--mongo.gcs.output.mode=errorifexists",
             "--mongo.gcs.output.location=gs://test"])
        mock_spark_session.read.format().option().option().option().load.return_value \
            = mock_spark_session.dataframe.DataFrame
        mongo_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format.assert_called_with(constants.FORMAT_MONGO)
        mock_spark_session.read \
            .format() \
            .option() \
            .option.assert_called_with(constants.MONGO_DATABASE,"database")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option.assert_called_with(constants.MONGO_COLLECTION,"collection")
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option() \
            .load.assert_called_with()
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_ERRORIFEXISTS)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .json.assert_called_once_with("gs://test")
