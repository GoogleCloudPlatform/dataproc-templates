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

from dataproc_templates.hive.util.hivesparkddl_to_bq import HiveSparkDDLToBigQueryTemplate
import dataproc_templates.util.template_constants as constants


class TestHiveSparkDDLToBigQueryTemplate:
    """
    Test suite for HiveSparkDDLToBigQueryTemplate
    """

    def test_parse_args(self):
        """Tests HiveSparkDDLToBigQueryTemplate.parse_args()"""

        hivesparkddl_to_bigquery_template = HiveSparkDDLToBigQueryTemplate()
        parsed_args = hivesparkddl_to_bigquery_template.parse_args(
            ["--hivesparkddl.bigquery.input.database=database",
             "--hivesparkddl.bigquery.output.dataset=dataset",
             "--hivesparkddl.bigquery.output.table=table",
             "--hivesparkddl.bigquery.output.bucket=bucket"])

        assert parsed_args["hivesparkddl.bigquery.input.database"] == "database"
        assert parsed_args["hivesparkddl.bigquery.output.dataset"] == "dataset"
        assert parsed_args["hivesparkddl.bigquery.output.table"] == "table"
        assert parsed_args["hivesparkddl.bigquery.output.bucket"] == "bucket"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run(self, mock_spark_session):
        """Tests HiveSparkDDLToBigQueryTemplate runs with overwrite mode"""

        hivesparkddl_to_bigquery_template = HiveSparkDDLToBigQueryTemplate()
        mock_parsed_args = hivesparkddl_to_bigquery_template.parse_args(
            ["--hivesparkddl.bigquery.input.database=database",
             "--hivesparkddl.bigquery.output.dataset=dataset",
             "--hivesparkddl.bigquery.output.table=table",
             "--hivesparkddl.bigquery.output.bucket=bucket"])
        mock_spark_session.table.return_value = mock_spark_session.dataframe.DataFrame
        hivesparkddl_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.table.assert_called_once_with("database")
        mock_spark_session.dataframe.DataFrame.write \
            .format.assert_called_once_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option.assert_called_once_with(constants.TEMP_GCS_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode() \
            .save.assert_called_once()
