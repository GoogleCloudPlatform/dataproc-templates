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

from dataproc_templates.elasticsearch.elasticsearch_to_bigtable import ElasticsearchToBigTableTemplate
import dataproc_templates.util.template_constants as constants


class TestElasticsearchToBigTableTemplate:
    """
    Test suite for ElasticsearchToBigTableTemplate
    """

    def test_parse_args(self):
        """Tests ElasticsearchToBigTableTemplate.parse_args()"""

        elasticsearch_to_bigtable_template = ElasticsearchToBigTableTemplate()
        parsed_args = elasticsearch_to_bigtable_template.parse_args(
            ["--es.bt.input.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--es.bt.input.index=demo",
             "--es.bt.input.user=demo",
             "--es.bt.input.password=demo",
             "--es.bt.hbase.catalog.json={key:value}"])

        assert parsed_args["es.bt.input.node"] == "xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243"
        assert parsed_args["es.bt.input.index"] == "demo"
        assert parsed_args["es.bt.input.user"] == "demo"
        assert parsed_args["es.bt.input.password"] == "demo"
        assert parsed_args["es.bt.hbase.catalog.json"] == '{key:value}'

    @mock.patch.object(pyspark.sql, 'SparkSession')
    @mock.patch("dataproc_templates.util.dataframe_reader_wrappers.rename_columns")
    def test_run(self, mock_rename_columns, mock_spark_session):
        """Tests ElasticsearchToBigTableTemplate run"""

        elasticsearch_to_bigtable_template = ElasticsearchToBigTableTemplate()
        mock_parsed_args = elasticsearch_to_bigtable_template.parse_args(
            ["--es.bt.input.node=xxxxxxxxxxxx.us-central1.gcp.cloud.es.io:9243",
             "--es.bt.input.index=demo",
             "--es.bt.input.user=demo",
             "--es.bt.input.password=demo",
             "--es.bt.hbase.catalog.json={key:value}"])

        mock_spark_session.sparkContext.newAPIHadoopRDD.return_value = mock_spark_session.rdd.RDD
        mock_spark_session.read.json.return_value = mock_spark_session.dataframe.DataFrame
        mock_rename_columns.return_value = mock_spark_session.dataframe.DataFrame
        elasticsearch_to_bigtable_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.sparkContext.newAPIHadoopRDD.assert_called_once()
        mock_spark_session.sparkContext.newAPIHadoopRDD().flatMap.assert_called_once()
        mock_spark_session.read.json.assert_called_once()
        mock_rename_columns.assert_called_once()
        mock_spark_session.dataframe.DataFrame.write.format. \
            assert_called_once_with(constants.FORMAT_HBASE)
        mock_spark_session.dataframe.DataFrame.write.format().options. \
            assert_called_with(catalog='{key:value}')
        mock_spark_session.dataframe.DataFrame.write.format().options().option. \
            assert_called_once_with('hbase.spark.use.hbasecontext', "false")
