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
            ["--bigquery.memorystore.input.table=projectId:dataset.table",
             "--bigquery.memorystore.output.host=10.0.0.1",
             "--bigquery.memorystore.output.port=6379",
             "--bigquery.memorystore.output.table=output_table",
             "--bigquery.memorystore.output.key.column=id",
             "--bigquery.memorystore.output.model=hash",
             "--bigquery.memorystore.output.mode=overwrite",
             "--bigquery.memorystore.output.ttl=100",
             "--bigquery.memorystore.output.dbnum=1"
             ])

        assert parsed_args[constants.BQ_MEMORYSTORE_INPUT_TABLE] == "projectId:dataset.table"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_HOST] == "10.0.0.1"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_PORT] == "6379"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_TABLE] == "output_table"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_KEY_COLUMN] == "id"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_MODEL] == "hash"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_MODE] == "overwrite"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_TTL] == "100"
        assert parsed_args[constants.BQ_MEMORYSTORE_OUTPUT_DBNUM] == "1"


    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run(self, mock_spark_session):
        """Tests BigQueryToMemorystoreTemplate runs"""

        bigquery_to_memorystore_template = BigQueryToMemorystoreTemplate()
        mock_parsed_args = bigquery_to_memorystore_template.parse_args(
            ["--bigquery.memorystore.input.table=projectId:dataset.table",
             "--bigquery.memorystore.output.host=10.0.0.1",
             "--bigquery.memorystore.output.port=6379",
             "--bigquery.memorystore.output.table=output_table",
             "--bigquery.memorystore.output.key.column=id",
             "--bigquery.memorystore.output.model=hash",
             "--bigquery.memorystore.output.mode=append",
             "--bigquery.memorystore.output.ttl=0",
             "--bigquery.memorystore.output.dbnum=0"
             ])

        # Correctly mock the chained calls
        mock_writer = mock.Mock()  # Mock for DataFrameWriter
        mock_df = mock.Mock()      # Mock for DataFrame
        mock_spark_session.read.format.return_value = mock_df
        mock_df.option.return_value = mock_df  # Chain .option() calls
        mock_df.load.return_value = mock_df
        mock_df.write.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer  # Chain .option() calls
        mock_writer.mode.return_value = mock_writer  # Chain .mode() call


        bigquery_to_memorystore_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format.assert_called_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.read \
            .format() \
            .option.assert_called_with(constants.TABLE, "projectId:dataset.table")
        mock_spark_session.read \
            .format() \
            .option() \
            .load.assert_called_with()

        mock_df.write.format.assert_called_with(constants.FORMAT_MEMORYSTORE)

        # Get calls
        write_calls = mock_writer.option.call_args_list
        # Extract output options from calls
        output_options = {call[0][0]: call[0][1] for call in write_calls if len(call[0]) > 1}

        # Assert output options (using stripped keys)
        assert output_options['host'] == "10.0.0.1"
        assert output_options['port'] == "6379"
        assert output_options['table'] == "output_table"
        assert output_options['key.column'] == "id"
        assert output_options['model'] == "hash"
        assert output_options['dbNum'] == "0"
        assert output_options['ttl'] == "0"

        mock_writer.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_writer.save.assert_called_once_with()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_binary_model(self, mock_spark_session):
        """Tests BigQueryToMemorystoreTemplate runs with binary model"""

        bigquery_to_memorystore_template = BigQueryToMemorystoreTemplate()
        mock_parsed_args = bigquery_to_memorystore_template.parse_args(
            ["--bigquery.memorystore.input.table=projectId:dataset.table",
             "--bigquery.memorystore.output.host=10.0.0.1",
             "--bigquery.memorystore.output.port=6379",
             "--bigquery.memorystore.output.table=output_table",
             "--bigquery.memorystore.output.key.column=id",
             "--bigquery.memorystore.output.model=binary",
             "--bigquery.memorystore.output.mode=append",
             "--bigquery.memorystore.output.ttl=0",
             "--bigquery.memorystore.output.dbnum=0"
             ])

        # Correctly mock the chained calls
        mock_writer = mock.Mock()  # Mock for DataFrameWriter
        mock_df = mock.Mock()      # Mock for DataFrame
        mock_spark_session.read.format.return_value = mock_df
        mock_df.option.return_value = mock_df  # Chain .option() calls
        mock_df.load.return_value = mock_df
        mock_df.write.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer  # Chain .option() calls
        mock_writer.mode.return_value = mock_writer

        bigquery_to_memorystore_template.run(mock_spark_session, mock_parsed_args)
        # Get calls
        write_calls = mock_writer.option.call_args_list
        # Extract output options from calls
        output_options = {call[0][0]: call[0][1] for call in write_calls if len(call[0]) > 1}

        # Assert output options (using stripped keys)
        assert output_options['model'] == "binary"