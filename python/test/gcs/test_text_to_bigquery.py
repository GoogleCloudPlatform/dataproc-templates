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

from dataproc_templates.gcs.text_to_bigquery import TextToBigQueryTemplate
import dataproc_templates.util.template_constants as constants


class TestTextToBigQueryTemplate:
    """
    Test suite for TextToBigQueryTemplate
    """

    def test_parse_args(self):
        """Tests TextToBigQueryTemplate.parse_args()"""

        text_to_bigquery_template = TextToBigQueryTemplate()
        parsed_args = text_to_bigquery_template.parse_args(
            ["--text.bigquery.input.compression=bzip2",
             "--text.bigquery.output.mode=append",
             "--text.bigquery.input.location=gs://test",
             "--text.bigquery.output.dataset=dataset",
             "--text.bigquery.output.table=table",
             "--text.bigquery.temp.bucket.name=bucket",
             "--text.bigquery.input.sep=|"])

        assert parsed_args["text.bigquery.input.compression"] == "bzip2"
        assert parsed_args["text.bigquery.output.mode"] == "append"
        assert parsed_args["text.bigquery.input.location"] == "gs://test"
        assert parsed_args["text.bigquery.output.dataset"] == "dataset"
        assert parsed_args["text.bigquery.output.table"] == "table"
        assert parsed_args["text.bigquery.temp.bucket.name"] == "bucket"
        assert parsed_args["text.bigquery.input.sep"] == "|"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_bzip2(self, mock_spark_session):
        """Tests TextToBigqueryTemplate runs with | delimiter and bzip2 compression format"""

        text_to_bigquery_template = TextToBigQueryTemplate()
        mock_parsed_args = text_to_bigquery_template.parse_args(
            ["--text.bigquery.input.location=gs://test",
             "--text.bigquery.input.header=false",
             "--text.bigquery.output.dataset=dataset",
             "--text.bigquery.output.table=table",
             "--text.bigquery.input.compression=bzip2",
             "--text.bigquery.temp.bucket.name=bucket",
             "--text.bigquery.output.mode=ignore",
             "--text.bigquery.input.sep=|"])

        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        text_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_CSV)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.CSV_HEADER: 'false',
            constants.CSV_INFER_SCHEMA: 'true',
            constants.CSV_SEP: "|",
        })
        mock_spark_session.read.format().options().load.assert_called_once_with("gs://test")

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
    def test_run_lz4(self, mock_spark_session):
        """Tests TextToBigqueryTemplate runs with | delimiter and lz4 compression format"""

        text_to_bigquery_template = TextToBigQueryTemplate()
        mock_parsed_args = text_to_bigquery_template.parse_args(
            ["--text.bigquery.input.location=gs://test",
             "--text.bigquery.input.header=false",
             "--text.bigquery.input.compression=lz4",
             "--text.bigquery.input.comment=#",
             "--text.bigquery.input.timestampntzformat=yyyy-MM-dd'T'HH:mm:ss",
             "--text.bigquery.output.dataset=dataset",
             "--text.bigquery.output.table=table",
             "--text.bigquery.temp.bucket.name=bucket",
             "--text.bigquery.output.mode=ignore",
             "--text.bigquery.input.sep=|"])

        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        text_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_CSV)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.CSV_HEADER: 'false',
            constants.CSV_INFER_SCHEMA: 'true',
            constants.CSV_SEP: "|",
            constants.CSV_COMMENT: "#",
            constants.CSV_TIMESTAMPNTZFORMAT: "yyyy-MM-dd'T'HH:mm:ss",
        })
        mock_spark_session.read.format().options().load.assert_called_once_with("gs://test")

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
    def test_run_deflate(self, mock_spark_session):
        """Tests TextToBigqueryTemplate runs with | delimiter and deflate compression format"""

        text_to_bigquery_template = TextToBigQueryTemplate()
        mock_parsed_args = text_to_bigquery_template.parse_args(
            ["--text.bigquery.input.location=gs://test",
             "--text.bigquery.input.header=false",
             "--text.bigquery.input.compression=deflate",
             "--text.bigquery.input.mode=FAILFAST",
             "--text.bigquery.output.dataset=dataset",
             "--text.bigquery.output.table=table",
             "--text.bigquery.temp.bucket.name=bucket",
             "--text.bigquery.output.mode=ignore",
             "--text.bigquery.input.sep=|"])

        mock_spark_session.read.format().options().load.return_value = mock_spark_session.dataframe.DataFrame
        text_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_CSV)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.CSV_HEADER: 'false',
            constants.CSV_INFER_SCHEMA: 'true',
            constants.CSV_MODE: 'FAILFAST',
            constants.CSV_SEP: "|",
        })
        mock_spark_session.read.format().options().load.assert_called_once_with("gs://test")

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
    def test_run_gzip(self, mock_spark_session):
        """Tests TextToBigqueryTemplate runs with | delimiter and gzip compression format"""

        text_to_bigquery_template = TextToBigQueryTemplate()
        mock_parsed_args = text_to_bigquery_template.parse_args(
            ["--text.bigquery.input.location=gs://test",
             "--text.bigquery.input.maxcolumns=100",
             "--text.bigquery.input.maxcharspercolumn=64",
             "--text.bigquery.input.compression=gzip",
             "--text.bigquery.output.dataset=dataset",
             "--text.bigquery.output.table=table",
             "--text.bigquery.temp.bucket.name=bucket",
             "--text.bigquery.output.mode=ignore",
             "--text.bigquery.input.sep=|"])
        mock_spark_session.read.format().options(
        ).load.return_value = mock_spark_session.dataframe.DataFrame
        text_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read.format.assert_called_with(
            constants.FORMAT_CSV)
        mock_spark_session.read.format().options.assert_called_with(**{
            constants.CSV_HEADER: 'true',
            constants.CSV_INFER_SCHEMA: 'true',
            constants.CSV_MAXCOLUMNS: '100',
            constants.CSV_MAXCHARSPERCOLUMN: '64',
            constants.CSV_SEP: "|",
        })
        mock_spark_session.read.format().options().load.assert_called_once_with("gs://test")

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
