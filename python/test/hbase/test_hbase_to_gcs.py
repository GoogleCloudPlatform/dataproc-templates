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

from dataproc_templates.hbase.hbase_to_gcs import HbaseToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestHbaseToGCSTemplate:
    """
    Test suite for HbaseToGCSTemplate
    """

    def test_parse_args(self):
        """Tests HbaseToGCSTemplate.parse_args()"""

        hive_to_gcs_template = HbaseToGCSTemplate()
        parsed_args = hive_to_gcs_template.parse_args(
            ["--hbase.gcs.output.location=gs://test",
             "--hbase.gcs.output.format=parquet",
             "--hbase.gcs.output.mode=overwrite",
             "--hbase.gcs.catalog.json={key:value}"])

        assert parsed_args["hbase.gcs.output.location"] == "gs://test"
        assert parsed_args["hbase.gcs.output.format"] == "parquet"
        assert parsed_args["hbase.gcs.output.mode"] == "overwrite"
        assert parsed_args["hbase.gcs.catalog.json"] == '{key:value}'

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run(self, mock_spark_session):
        """Tests HbaseToGCSTemplate runs for parquet format output"""

        hbase_to_gcs_template = HbaseToGCSTemplate()
        mock_parsed_args = hbase_to_gcs_template.parse_args(
            ["--hbase.gcs.output.location=gs://test",
             "--hbase.gcs.output.format=parquet",
             "--hbase.gcs.output.mode=overwrite",
             "--hbase.gcs.catalog.json={key:value}"])
        mock_spark_session.read.format() \
                .options().option() \
                .load.return_value = mock_spark_session.dataframe.DataFrame
        hbase_to_gcs_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format \
                .assert_called_with(constants.FORMAT_HBASE)
        mock_spark_session.read.format() \
                .options.assert_called_with(catalog='{key:value}')
        mock_spark_session.read.format() \
                .options().option.assert_called_with('hbase.spark.use.hbasecontext', "false")
        mock_spark_session.read.format() \
                .options().option().load.assert_called_once_with()

        mock_spark_session.dataframe.DataFrame.write \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write \
            .mode() \
            .parquet.assert_called_once_with("gs://test")
