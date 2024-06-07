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

from dataproc_templates.elasticsearch.elasticsearch_to_bq import ElasticsearchToBQTemplate
import dataproc_templates.util.template_constants as constants


class TestElasticsearchToBQTemplate:
    """
    Test suite for ElasticsearchToBQTemplate
    """

    def test_parse_args(self):
        """Tests ElasticsearchToBQTemplate.parse_args()"""

        elasticsearch_to_bigquery_template = ElasticsearchToBQTemplate()
        parsed_args = elasticsearch_to_bigquery_template.parse_args(
            ["--es.bq.input.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--es.bq.input.index=demo",
             "--es.bq.input.user=demo",
             "--es.bq.input.password=demo",
             "--es.bq.output.dataset=dataset",
             "--es.bq.output.table=table",
             "--es.bq.output.mode=append",
             "--es.bq.temp.bucket.name=bucket"])

        assert parsed_args["es.bq.input.node"] == "xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243"
        assert parsed_args["es.bq.input.index"] == "demo"
        assert parsed_args["es.bq.input.user"] == "demo"
        assert parsed_args["es.bq.input.password"] == "demo"
        assert parsed_args["es.bq.output.dataset"] == "dataset"
        assert parsed_args["es.bq.output.table"] == "table"
        assert parsed_args["es.bq.output.mode"] == "append"
        assert parsed_args["es.bq.temp.bucket.name"] == "bucket"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch("dataproc_templates.util.dataframe_reader_wrappers.rename_columns")
    def test_run(self, mock_rename_columns, mock_spark_session):
        """Tests ElasticsearchToBQTemplate run"""

        elasticsearch_to_bigquery_template = ElasticsearchToBQTemplate()
        mock_parsed_args = elasticsearch_to_bigquery_template.parse_args(
            ["--es.bq.input.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--es.bq.input.index=demo",
             "--es.bq.input.user=demo",
             "--es.bq.input.password=demo",
             "--es.bq.output.dataset=dataset",
             "--es.bq.output.table=table",
             "--es.bq.output.mode=append",
             "--es.bq.temp.bucket.name=bucket"])

        mock_spark_session.sparkContext.newAPIHadoopRDD.return_value = mock_spark_session.rdd.RDD
        mock_spark_session.read.json.return_value = mock_spark_session.dataframe.DataFrame
        mock_rename_columns.return_value = mock_spark_session.dataframe.DataFrame
        elasticsearch_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.sparkContext.newAPIHadoopRDD.assert_called_once()
        mock_spark_session.sparkContext.newAPIHadoopRDD().flatMap.assert_called_once()
        mock_spark_session.read.json.assert_called_once()
        mock_rename_columns.assert_called_once()
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option.assert_called_once_with(constants.ES_BQ_TEMP_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option().option.assert_called_once_with('enableListInference', True)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().option().mode().save.assert_called_once()
