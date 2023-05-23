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

from dataproc_templates.gcs.gcs_to_bigquery import GCSToBigQueryTemplate
import dataproc_templates.util.template_constants as constants


class TestGCSToBigQueryTemplate:
    """
    Test suite for GCSToBigQueryTemplate
    """

    def test_parse_args(self):
        """Tests GCSToBigQueryTemplate.parse_args()"""

        gcs_to_bigquery_template = GCSToBigQueryTemplate()
        parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=parquet",
             "--gcs.bigquery.output.mode=append",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])

        assert parsed_args["gcs.bigquery.input.format"] == "parquet"
        assert parsed_args["gcs.bigquery.output.mode"] == "append"
        assert parsed_args["gcs.bigquery.input.location"] == "gs://test"
        assert parsed_args["gcs.bigquery.output.dataset"] == "dataset"
        assert parsed_args["gcs.bigquery.output.table"] == "table"
        assert parsed_args["gcs.bigquery.temp.bucket.name"] == "bucket"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_parquet(self, mock_spark_session):
        """Tests GCSToBigqueryTemplate runs with parquet format"""

        gcs_to_bigquery_template = GCSToBigQueryTemplate()
        mock_parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=parquet",
             "--gcs.bigquery.output.mode=append",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])
        mock_spark_session.read.parquet.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

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


    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_json(self, mock_spark_session):
        """Tests GCSToBigqueryTemplate runs with json format"""

        gcs_to_bigquery_template = GCSToBigQueryTemplate()
        mock_parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=json",
             "--gcs.bigquery.output.mode=errorifexists",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])
        mock_spark_session.read.json.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.json.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option.assert_called_once_with(constants.GCS_BQ_TEMP_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_ERRORIFEXISTS)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_avro(self, mock_spark_session):
        """Tests GCSToBigqueryTemplate runs with avro"""

        gcs_to_bigquery_template = GCSToBigQueryTemplate()
        mock_parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=avro",
             "--gcs.bigquery.output.mode=overwrite",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])
        mock_spark_session.read.format().load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_AVRO_EXTD)
        mock_spark_session.read.format().load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option.assert_called_once_with(constants.GCS_BQ_TEMP_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_delta(self, mock_spark_session):
        """Tests GCSToBigqueryTemplate runs with delta format"""

        gcs_to_bigquery_template = GCSToBigQueryTemplate()
        mock_parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=delta",
             "--gcs.bigquery.output.mode=overwrite",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])
        mock_spark_session.read.format().load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_DELTA)
        mock_spark_session.read.format().load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option.assert_called_once_with(constants.GCS_BQ_TEMP_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_csv1(self, mock_spark_session):
        """Tests GCSToBigqueryTemplate runs with csv format"""

        gcs_to_bigquery_template = GCSToBigQueryTemplate()
        mock_parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=csv",
             "--gcs.bigquery.output.mode=ignore",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])
        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_CSV)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.CSV_HEADER: 'true',
            constants.CSV_INFER_SCHEMA: 'true',
        })
        mock_spark_session.read.format().options(
        ).load.assert_called_once_with("gs://test")

        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option.assert_called_once_with(constants.GCS_BQ_TEMP_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_IGNORE)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode().save.assert_called_once()

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_csv2(self, mock_spark_session):
        """Tests GCSToBigqueryTemplate runs with csv format and some optional csv options"""

        gcs_to_bigquery_template = GCSToBigQueryTemplate()
        mock_parsed_args = gcs_to_bigquery_template.parse_args(
            ["--gcs.bigquery.input.format=csv",
             "--gcs.bigquery.output.mode=ignore",
             "--gcs.bigquery.input.location=gs://test",
             "--gcs.bigquery.input.inferschema=false",
             "--gcs.bigquery.input.sep=|",
             "--gcs.bigquery.input.comment=#",
             "--gcs.bigquery.input.timestampntzformat=yyyy-MM-dd'T'HH:mm:ss",
             "--gcs.bigquery.output.dataset=dataset",
             "--gcs.bigquery.output.table=table",
             "--gcs.bigquery.temp.bucket.name=bucket"])
        mock_spark_session.read.format().options(
        ).load.return_value = mock_spark_session.dataframe.DataFrame
        gcs_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_CSV)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.CSV_HEADER: 'true',
            constants.CSV_INFER_SCHEMA: 'false',
            constants.CSV_SEP: "|",
            constants.CSV_COMMENT: "#",
            constants.CSV_TIMESTAMPNTZFORMAT: "yyyy-MM-dd'T'HH:mm:ss",
        })
        mock_spark_session.read.format().options(
        ).load.assert_called_once_with("gs://test")
        mock_spark_session.dataframe.DataFrame.write.format.assert_called_once_with(
            constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write.format().option(
        ).option.assert_called_once_with(constants.GCS_BQ_TEMP_BUCKET, "bucket")
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode.assert_called_once_with(constants.OUTPUT_MODE_IGNORE)
        mock_spark_session.dataframe.DataFrame.write.format(
        ).option().option().mode().save.assert_called_once()

