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

import pyspark
from unittest import mock

from dataproc_templates.bigquery.bigquery_to_memorystore import BigQueryToMemorystoreTemplate
import dataproc_templates.util.template_constants as constants


class TestBigQueryToMemorystoreTemplate:
    """
    Test suite for BigQueryToMemorystoreTemplate
    """

    def test_parse_args(self):
        """Tests BigQueryToMemorystoreTemplate.parse_args()"""

        bigquery_to_memorystore_template = BigQueryToMemorystoreTemplate()
        parsed_args = bigquery_to_memorystore_template.parse_args(
            [
                "--bigquery.memorystore.input.table=projectId.dataset.table",
                "--bigquery.memorystore.output.host=redis_host",
                "--bigquery.memorystore.output.table=redis_table",
                "--bigquery.memorystore.output.key.column=key_column",
            ]
        )

        assert parsed_args[constants.BQ_MEMORYSTORE_INPUT_TABLE] == "projectId.dataset.table"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_HOST] == "redis_host"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_TABLE] == "redis_table"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_KEY_COLUMN] == "key_column"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_PORT] == 6379
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_MODEL] == constants.BQ_MEMORYSTORE_OUTPUT_MODEL_HASH
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_MODE] == constants.OUTPUT_MODE_APPEND
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_TTL] == 0
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_DBNUM] == 0

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_hash_model(self, mock_spark_session):
        """Tests BigQueryToMemorystoreTemplate runs for hash model"""

        bigquery_to_memorystore_template = BigQueryToMemorystoreTemplate()
        mock_parsed_args = bigquery_to_memorystore_template.parse_args(
            [
                "--bigquery.memorystore.input.table=projectId.dataset.table",
                "--bigquery.memorystore.output.host=redis_host",
                "--bigquery.memorystore.output.table=redis_table",
                "--bigquery.memorystore.output.key.column=key_column",
                "--bigquery.memorystore.output.model=hash",
                "--bigquery.memorystore.output.mode=overwrite",
            ]
        )
        mock_spark_session.read.format().option().load.return_value = mock_spark_session.dataframe.DataFrame

        bigquery_to_memorystore_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.read.format().option.assert_called_with(constants.TABLE, "projectId.dataset.table")
        mock_spark_session.read.format().option().load.assert_called_once()

        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(constants.FORMAT_MEMORYSTORE)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.TABLE, "redis_table")
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_KEY_COLUMN, "key_column")
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_MODEL, constants.BQ_MEMORYSTORE_OUTPUT_MODEL_HASH)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_HOST, "redis_host")
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_PORT, 6379)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_DBNUM, 0)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_TTL, 0)

        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write.mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_binary_model(self, mock_spark_session):
        """Tests BigQueryToMemorystoreTemplate runs for binary model"""

        bigquery_to_memorystore_template = BigQueryToMemorystoreTemplate()
        mock_parsed_args = bigquery_to_memorystore_template.parse_args(
            [
                "--bigquery.memorystore.input.table=projectId.dataset.table",
                "--bigquery.memorystore.output.host=redis_host",
                "--bigquery.memorystore.output.table=redis_table",
                "--bigquery.memorystore.output.key.column=key_column",
                "--bigquery.memorystore.output.model=binary",
                "--bigquery.memorystore.output.mode=append",
                "--bigquery.memorystore.output.ttl=60",
                "--bigquery.memorystore.output.dbnum=1",
            ]
        )
        mock_spark_session.read.format().option().load.return_value = mock_spark_session.dataframe.DataFrame

        bigquery_to_memorystore_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.read.format().option.assert_called_with(constants.TABLE, "projectId.dataset.table")
        mock_spark_session.read.format().option().load.assert_called_once()

        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(constants.FORMAT_MEMORYSTORE)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.TABLE, "redis_table")
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_KEY_COLUMN, "key_column")
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_MODEL, constants.BQ_MEMORYSTORE_OUTPUT_MODEL_BINARY)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_HOST, "redis_host")
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_PORT, 6379)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_DBNUM, 1)
        mock_spark_session.dataframe.DataFrame.write.format().option.assert_any_call(constants.MEMORYSTORE_TTL, 60)

        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().save.assert_called_once()