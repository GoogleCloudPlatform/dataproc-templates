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

from dataproc_templates.azure.azure_blob_storage_to_bigquery import AzureBlobStorageToBigQueryTemplate
import dataproc_templates.util.template_constants as constants


class TestAzureBlobStorageToBigQueryTemplate:
    """
    Test suite for AzureBlobStorageToBigQueryTemplate
    """

    def test_parse_args(self):
        """Tests AzureBlobStorageToBigQueryTemplate.parse_args()"""

        azure_blob_storage_to_bigquery_template = AzureBlobStorageToBigQueryTemplate()
        parsed_args = azure_blob_storage_to_bigquery_template.parse_args([
            "--azure.blob.bigquery.input.format=parquet",
            "--azure.blob.bigquery.output.mode=append",
            "--azure.blob.bigquery.input.location=gs://test",
            "--azure.blob.bigquery.output.dataset=dataset",
            "--azure.blob.bigquery.output.table=table",
            "--azure.blob.bigquery.temp.bucket.name=bucket",
            "--azure.blob.storage.account=test_storage_account",
            "--azure.blob.container.name=test_container",
            "--azure.blob.sas.token=test_sas_token"])

        assert parsed_args["azure.blob.bigquery.input.format"] == "parquet"
        assert parsed_args["azure.blob.bigquery.output.mode"] == "append"
        assert parsed_args["azure.blob.bigquery.input.location"] == "gs://test"
        assert parsed_args["azure.blob.bigquery.output.dataset"] == "dataset"
        assert parsed_args["azure.blob.bigquery.output.table"] == "table"
        assert parsed_args["azure.blob.bigquery.temp.bucket.name"] == "bucket"
        assert parsed_args["azure.blob.storage.account"] == "test_storage_account"
        assert parsed_args["azure.blob.container.name"] == "test_container"
        assert parsed_args["azure.blob.sas.token"] == "test_sas_token"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_parquet(self, mock_spark_session):
        """Tests GCSToBigqueryTemplate runs with parquet format"""

        azure_blob_storage_to_bigquery_template = AzureBlobStorageToBigQueryTemplate()
        mock_parsed_args = azure_blob_storage_to_bigquery_template.parse_args([
            "--azure.blob.bigquery.input.format=parquet",
            "--azure.blob.bigquery.output.mode=append",
            "--azure.blob.bigquery.input.location=gs://test",
            "--azure.blob.bigquery.output.dataset=dataset",
            "--azure.blob.bigquery.output.table=table",
            "--azure.blob.bigquery.temp.bucket.name=bucket",
            "--azure.blob.storage.account=test_storage_account",
            "--azure.blob.container.name=test_container",
            "--azure.blob.sas.token=test_sas_token"])
        mock_spark_session.read.parquet.return_value = mock_spark_session.dataframe.DataFrame
        azure_blob_storage_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.parquet.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option.assert_called_once_with(constants.GCS_BQ_TEMP_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode().save.assert_called_once()
