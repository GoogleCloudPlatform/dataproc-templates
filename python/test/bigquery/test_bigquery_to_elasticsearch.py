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

from dataproc_templates.bigquery.bigquery_to_elasticsearch import BigQueryToElasticsearchTemplate
import dataproc_templates.util.template_constants as constants


class TestBigQueryToElasticsearchTemplate:
    """
    Test suite for BigQueryToElasticsearchTemplate
    """

    def test_parse_args(self):
        """Tests BigQueryToElasticsearchTemplate.parse_args()"""

        bigquery_to_elasticsearch_template = BigQueryToElasticsearchTemplate()
        parsed_args = bigquery_to_elasticsearch_template.parse_args(
            ["--bigquery.elasticsearch.input.table=projectId:dataset.table",
             "--bigquery.elasticsearch.output.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--bigquery.elasticsearch.output.index=demo",
             "--bigquery.elasticsearch.output.user=demo",
             "--bigquery.elasticsearch.output.password=demo"])

        assert parsed_args["bigquery.elasticsearch.input.table"] == "projectId:dataset.table"
        assert parsed_args["bigquery.elasticsearch.output.node"] == "xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243"
        assert parsed_args["bigquery.elasticsearch.output.index"] == "demo"
        assert parsed_args["bigquery.elasticsearch.output.user"] == "demo"
        assert parsed_args["bigquery.elasticsearch.output.password"] == "demo"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run(self, mock_spark_session):
        """Tests BigQueryToElasticsearchTemplate run"""

        bigquery_to_elasticsearch_template = BigQueryToElasticsearchTemplate()
        mock_parsed_args = bigquery_to_elasticsearch_template.parse_args(
            ["--bigquery.elasticsearch.input.table=projectId:dataset.table",
             "--bigquery.elasticsearch.output.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--bigquery.elasticsearch.output.index=demo",
             "--bigquery.elasticsearch.output.user=demo",
             "--bigquery.elasticsearch.output.password=demo"])
        mock_spark_session.read.format().options().load.return_value \
            = mock_spark_session.dataframe.DataFrame
        bigquery_to_elasticsearch_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format.assert_called_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.read \
            .format() \
            .options.assert_called_with(**{"table": "projectId:dataset.table", "viewsEnabled": "true", "parentProject": "projectId"})
        mock_spark_session.read \
            .format() \
            .options() \
            .load.assert_called_with()
        mock_spark_session.dataframe.DataFrame.write \
            .format.assert_called_once_with(constants.FORMAT_ELASTICSEARCH_WRITER)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .mode() \
            .options.assert_called_once_with(**{"es.nodes": "xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
                                                "es.resource":"demo","es.net.http.auth.user":"demo",
                                                "es.net.http.auth.pass":"demo", "es.nodes.discovery": "true",
                                                "es.nodes.client.only": "false", "es.nodes.data.only": "true",
                                                "es.nodes.wan.only": "false", "es.http.timeout": "1m",
                                                "es.http.retries": "3", "es.action.heart.beat.lead": "15s",
                                                "es.net.ssl": "false", "es.net.ssl.cert.allow.self.signed": "false",
                                                "es.net.ssl.protocol": "TLS", "es.batch.size.bytes": "1mb",
                                                "es.batch.size.entries": "1000", "es.batch.write.retry.count": "3",
                                                "es.batch.write.retry.wait": "10s"})
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .mode() \
            .options() \
            .save.assert_called_once_with("demo")
