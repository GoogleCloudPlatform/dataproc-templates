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

from dataproc_templates.gcs.gcs_to_bigquery import GcsToBigQueryTemplate
import dataproc_templates.util.template_constants as constants


class TestGcsToBigQueryTemplate:
    """
    Test suite for GcsToBigQueryTempate
    """

    def test_parse_args(self):
        """Tests GcsToBigQueryTemplate.parse_args()"""

        gcs_to_bigquery_template = GcsToBigQueryTemplate()
        parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=parquet",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])

        assert parsed_args["gcs.bigquery.input.format"] == "parquet"
        assert parsed_args["gcs.bigquery.input.location"] == "gs://test"
        assert parsed_args["gcs.bigquery.output.dataset"] == "dataset"
        assert parsed_args["gcs.bigquery.output.table"] == "table"
        assert parsed_args["gcs.bigquery.temp.bucket.name"] == "bucket"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_parquet(self, mock_spark_session):
        """Tests GcsToBigqueryTemplate runs with parquet format"""

        gcs_to_bigquery_template = GcsToBigQueryTemplate()
        mock_parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=parquet",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])
        mock_spark_session.read.parquet.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.parquet.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.GCS_BQ_OUTPUT_FORMAT)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option.assert_called_once_with(constants.GCS_BQ_OUTPUT, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option.assert_called_once_with(constants.GCS_BQ_TEMP_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode.assert_called_once_with("append")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_avro(self, mock_spark_session):
        """Tests GcsToBigqueryTemplate runs with parquet avro"""

        gcs_to_bigquery_template = GcsToBigQueryTemplate()
        mock_parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=avro",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])
        mock_spark_session.read.format().load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.GCS_BQ_AVRO_EXTD_FORMAT)
        mock_spark_session.read.format().load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.GCS_BQ_OUTPUT_FORMAT)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option.assert_called_once_with(constants.GCS_BQ_OUTPUT, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option.assert_called_once_with(constants.GCS_BQ_TEMP_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode.assert_called_once_with("append")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_csv(self, mock_spark_session):
        """Tests GcsToBigqueryTemplate runs with csv format"""

        gcs_to_bigquery_template = GcsToBigQueryTemplate()
        mock_parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=csv",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])
        mock_spark_session.read.format().option().option(
        ).load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.GCS_BQ_CSV_FORMAT)
        mock_spark_session.read.format().option.assert_called_with(
            constants.GCS_BQ_CSV_HEADER, True)
        mock_spark_session.read.format().option().option.assert_called_with(
            constants.GCS_BQ_CSV_INFER_SCHEMA, True)
        mock_spark_session.read.format().option().option(
        ).load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.GCS_BQ_OUTPUT_FORMAT)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option.assert_called_once_with(constants.GCS_BQ_OUTPUT, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option.assert_called_once_with(constants.GCS_BQ_TEMP_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode.assert_called_once_with("append")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode().save.assert_called_once()
