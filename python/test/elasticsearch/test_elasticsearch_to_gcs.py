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

from dataproc_templates.elasticsearch.elasticsearch_to_gcs import ElasticsearchToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestElasticsearchToGCSTemplate:
    """
    Test suite for ElasticsearchToGCSTemplate
    """
    def test_parse_args(self):
        elasticsearch_to_gcs_template = ElasticsearchToGCSTemplate()
        parsed_args = elasticsearch_to_gcs_template.parse_args(
            ["--es.gcs.input.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--es.gcs.input.index=demo",
             "--es.gcs.input.user=demo",
             "--es.gcs.input.password=demo",
             "--es.gcs.output.format=parquet",
             "--es.gcs.output.mode=overwrite",
             "--es.gcs.output.location=gs://my-output/esgcsoutput"])

        assert parsed_args["es.gcs.input.node"] == 'xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243'
        assert parsed_args["es.gcs.input.index"] == 'demo'
        assert parsed_args["es.gcs.input.user"] == 'demo'
        assert parsed_args["es.gcs.input.password"] == 'demo'
        assert parsed_args["es.gcs.output.format"] == 'parquet'
        assert parsed_args["es.gcs.output.mode"] == 'overwrite'
        assert parsed_args["es.gcs.output.location"] == 'gs://my-output/esgcsoutput'

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch("dataproc_templates.util.dataframe_reader_wrappers.rename_columns")
    def test_run_parquet(self, mock_rename_columns, mock_spark_session):
        """Tests ElasticsearchToGCSTemplate runs with parquet format"""

        elasticsearch_to_gcs_template = ElasticsearchToGCSTemplate()
        mock_parsed_args = elasticsearch_to_gcs_template.parse_args(
            ["--es.gcs.input.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--es.gcs.input.index=demo",
             "--es.gcs.input.user=demo",
             "--es.gcs.input.password=demo",
             "--es.gcs.output.format=parquet",
             "--es.gcs.output.mode=overwrite",
             "--es.gcs.output.location=gs://my-output/esgcsoutput"])

        mock_spark_session.sparkContext.newAPIHadoopRDD.return_value = mock_spark_session.rdd.RDD
        mock_spark_session.read.json.return_value = mock_spark_session.dataframe.DataFrame
        mock_rename_columns.return_value = mock_spark_session.dataframe.DataFrame
        elasticsearch_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.sparkContext.newAPIHadoopRDD.assert_called_once()
        mock_spark_session.sparkContext.newAPIHadoopRDD().flatMap.assert_called_once()
        mock_spark_session.read.json.assert_called_once()
        mock_rename_columns.assert_called_once()
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .parquet.assert_called_once_with("gs://my-output/esgcsoutput")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch("dataproc_templates.util.dataframe_reader_wrappers.rename_columns")
    def test_run_csv(self, mock_rename_columns, mock_spark_session):
        """Tests ElasticsearchToGCSTemplate runs with csv format"""

        elasticsearch_to_gcs_template = ElasticsearchToGCSTemplate()
        mock_parsed_args = elasticsearch_to_gcs_template.parse_args(
            ["--es.gcs.input.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--es.gcs.input.index=demo",
             "--es.gcs.input.user=demo",
             "--es.gcs.input.password=demo",
             "--es.gcs.output.format=csv",
             "--es.gcs.output.mode=overwrite",
             "--es.gcs.output.location=gs://my-output/esgcsoutput"])

        mock_spark_session.sparkContext.newAPIHadoopRDD.return_value = mock_spark_session.rdd.RDD
        mock_spark_session.read.json.return_value = mock_spark_session.dataframe.DataFrame
        mock_rename_columns.return_value = mock_spark_session.dataframe.DataFrame
        elasticsearch_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.sparkContext.newAPIHadoopRDD.assert_called_once()
        mock_spark_session.sparkContext.newAPIHadoopRDD().flatMap.assert_called_once()
        mock_spark_session.read.json.assert_called_once()
        mock_rename_columns.assert_called_once()
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .options.assert_called_once_with(**{constants.CSV_HEADER: 'true'})

        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .options() \
            .csv.assert_called_once_with("gs://my-output/esgcsoutput")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch("dataproc_templates.util.dataframe_reader_wrappers.rename_columns")
    def test_run_avro(self, mock_rename_columns, mock_spark_session):
        """Tests ElasticsearchToGCSTemplate runs with avro format"""

        elasticsearch_to_gcs_template = ElasticsearchToGCSTemplate()
        mock_parsed_args = elasticsearch_to_gcs_template.parse_args(
            ["--es.gcs.input.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--es.gcs.input.index=demo",
             "--es.gcs.input.user=demo",
             "--es.gcs.input.password=demo",
             "--es.gcs.output.format=avro",
             "--es.gcs.output.mode=overwrite",
             "--es.gcs.output.location=gs://my-output/esgcsoutput"])

        mock_spark_session.sparkContext.newAPIHadoopRDD.return_value = mock_spark_session.rdd.RDD
        mock_spark_session.read.json.return_value = mock_spark_session.dataframe.DataFrame
        mock_rename_columns.return_value = mock_spark_session.dataframe.DataFrame
        elasticsearch_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.sparkContext.newAPIHadoopRDD.assert_called_once()
        mock_spark_session.sparkContext.newAPIHadoopRDD().flatMap.assert_called_once()
        mock_spark_session.read.json.assert_called_once()
        mock_rename_columns.assert_called_once()
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .format.assert_called_once_with(constants.FORMAT_AVRO)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .format() \
            .save.assert_called_once_with("gs://my-output/esgcsoutput")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch("dataproc_templates.util.dataframe_reader_wrappers.rename_columns")
    def test_run_json(self, mock_rename_columns, mock_spark_session):
        """Tests ElasticsearchToGCSTemplate runs with json format"""

        elasticsearch_to_gcs_template = ElasticsearchToGCSTemplate()
        mock_parsed_args = elasticsearch_to_gcs_template.parse_args(
            ["--es.gcs.input.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--es.gcs.input.index=demo",
             "--es.gcs.input.user=demo",
             "--es.gcs.input.password=demo",
             "--es.gcs.output.format=json",
             "--es.gcs.output.mode=overwrite",
             "--es.gcs.output.location=gs://my-output/esgcsoutput"])

        mock_spark_session.sparkContext.newAPIHadoopRDD.return_value = mock_spark_session.rdd.RDD
        mock_spark_session.read.json.return_value = mock_spark_session.dataframe.DataFrame
        mock_rename_columns.return_value = mock_spark_session.dataframe.DataFrame
        elasticsearch_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.sparkContext.newAPIHadoopRDD.assert_called_once()
        mock_spark_session.sparkContext.newAPIHadoopRDD().flatMap.assert_called_once()
        mock_spark_session.read.json.assert_called_once()
        mock_rename_columns.assert_called_once()
        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .json.assert_called_once_with("gs://my-output/esgcsoutput")
