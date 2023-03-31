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

from dataproc_templates.gcs.gcs_to_bigtable import GCSToBigTableTemplate
import dataproc_templates.util.template_constants as constants


class TestGCSToBigTableTemplate:
    """
    Test suite for GCSToBigTableTemplate
    """

    def test_parse_args(self):
        """Tests GCSToBigTableTemplate.parse_args()"""

        gcs_to_bigtable_template = GCSToBigTableTemplate()
        parsed_args = gcs_to_bigtable_template.parse_args(
            ["--gcs.bigtable.input.format=parquet",
             "--gcs.bigtable.input.location=gs://test",
             "--gcs.bigtable.hbase.catalog.json={key:value}"])

        assert parsed_args["gcs.bigtable.input.format"] == "parquet"
        assert parsed_args["gcs.bigtable.input.location"] == "gs://test"
        assert parsed_args["gcs.bigtable.hbase.catalog.json"] == '{key:value}'

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run(self, mock_spark_session):
        """Tests GCSToBigTableTemplate runs"""

        gcs_to_bigtable_template = GCSToBigTableTemplate()
        mock_parsed_args = gcs_to_bigtable_template.parse_args(
            ["--gcs.bigtable.input.format=parquet",
             "--gcs.bigtable.input.location=gs://test",
             "--gcs.bigtable.hbase.catalog.json={key:value}"])
        mock_spark_session.read.parquet.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigtable_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.parquet.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format. \
            assert_called_once_with(constants.FORMAT_HBASE)
        mock_spark_session.dataframe.DataFrame.write.format().options. \
            assert_called_with(catalog='{key:value}')
        mock_spark_session.dataframe.DataFrame.write.format().options().option. \
            assert_called_once_with('hbase.spark.use.hbasecontext', "false")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_csv1(self, mock_spark_session):
        """Tests GCSToBigTableTemplate runs with csv format"""

        gcs_to_bigtable_template = GCSToBigTableTemplate()
        mock_parsed_args = gcs_to_bigtable_template.parse_args(
            ["--gcs.bigtable.input.format=csv",
             "--gcs.bigtable.input.location=gs://test",
             "--gcs.bigtable.input.header=false",
             "--gcs.bigtable.hbase.catalog.json={key:value}"])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigtable_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_CSV)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.CSV_HEADER: 'false',
            constants.CSV_INFER_SCHEMA: 'true',
        })
        mock_spark_session.read.format().options().load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format. \
            assert_called_once_with(constants.FORMAT_HBASE)
        mock_spark_session.dataframe.DataFrame.write.format().options. \
            assert_called_with(catalog='{key:value}')
        mock_spark_session.dataframe.DataFrame.write.format().options().option. \
            assert_called_once_with('hbase.spark.use.hbasecontext', "false")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_csv2(self, mock_spark_session):
        """Tests GCSToBigTableTemplate runs with csv format and some optional csv options"""

        gcs_to_bigtable_template = GCSToBigTableTemplate()
        mock_parsed_args = gcs_to_bigtable_template.parse_args(
            ["--gcs.bigtable.input.format=csv",
             "--gcs.bigtable.input.location=gs://test",
             "--gcs.bigtable.input.inferschema=false",
             "--gcs.bigtable.input.sep=|",
             "--gcs.bigtable.input.comment=#",
             "--gcs.bigtable.input.timestampntzformat=yyyy-MM-dd'T'HH:mm:ss",
             "--gcs.bigtable.hbase.catalog.json={key:value}"])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigtable_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_CSV)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.CSV_HEADER: 'true',
            constants.CSV_INFER_SCHEMA: 'false',
            constants.CSV_SEP: "|",
            constants.CSV_COMMENT: "#",
            constants.CSV_TIMESTAMPNTZFORMAT: "yyyy-MM-dd'T'HH:mm:ss",
        })
        mock_spark_session.read.format().options().load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format. \
            assert_called_once_with(constants.FORMAT_HBASE)
        mock_spark_session.dataframe.DataFrame.write.format().options. \
            assert_called_with(catalog='{key:value}')
        mock_spark_session.dataframe.DataFrame.write.format().options().option. \
            assert_called_once_with('hbase.spark.use.hbasecontext', "false")
