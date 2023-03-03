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
import pyspark.sql

from dataproc_templates.s3.s3_to_bigquery import S3ToBigQueryTemplate
import dataproc_templates.util.template_constants as constants


class TestS3ToBigQueryTemplate:
    """
    Test suite for S3ToBigQueryTemplate
    """

    def test_parse_args1(self):
        """Tests S3ToBigQueryTemplate.parse_args()"""

        s3_to_bigquery_template = S3ToBigQueryTemplate()
        parsed_args = s3_to_bigquery_template.parse_args(
            ["--s3.bq.input.location=s3a://bucket/file",
             "--s3.bq.access.key=SomeAccessKey",
             "--s3.bq.secret.key=SomeSecretKey",
             "--s3.bq.input.format=csv",
             "--s3.bq.output.dataset.name=xyz",
             "--s3.bq.output.table.name=lmn",
             "--s3.bq.temp.bucket.name=test",
             "--s3.bq.output.mode=append"
             ])

        assert parsed_args["s3.bq.input.location"] == "s3a://bucket/file"
        assert parsed_args["s3.bq.access.key"] == "SomeAccessKey"
        assert parsed_args["s3.bq.secret.key"] == "SomeSecretKey"
        assert parsed_args["s3.bq.input.format"] == "csv"
        assert parsed_args["s3.bq.output.dataset.name"] == "xyz"
        assert parsed_args["s3.bq.output.table.name"] == "lmn"
        assert parsed_args["s3.bq.temp.bucket.name"] == "test"
        assert parsed_args["s3.bq.output.mode"] == "append"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args2(self, mock_spark_session):
        """Tests S3ToBigQueryTemplate reads parquet"""

        s3_to_bigquery_template = S3ToBigQueryTemplate()

        mock_parsed_args = s3_to_bigquery_template.parse_args(
            ["--s3.bq.input.location=s3a://bucket/file",
             "--s3.bq.access.key=SomeAccessKey",
             "--s3.bq.secret.key=SomeSecretKey",
             "--s3.bq.input.format=parquet",
             "--s3.bq.output.dataset.name=xyz",
             "--s3.bq.output.table.name=lmn",
             "--s3.bq.temp.bucket.name=test",
             "--s3.bq.output.mode=append"
             ])
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3ACCESSKEY, "SomeAccessKey")
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3SECRETKEY, "SomeSecretKey")
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3ENDPOINT, constants.S3_BQ_ENDPOINT_VALUE)

        mock_spark_session.read \
            .parquet \
            .return_value = mock_spark_session.dataframe.DataFrame
        s3_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .parquet \
            .assert_called_with("s3a://bucket/file")

        mock_spark_session.dataframe.DataFrame.write \
            .format \
            .assert_called_once_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option \
            .assert_called_once_with(constants.TABLE, "xyz.lmn")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option \
            .assert_called_once_with(constants.TEMP_GCS_BUCKET, "test")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode \
            .assert_called_once_with(constants.OUTPUT_MODE_APPEND)

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args3(self, mock_spark_session):
        """Tests S3ToBigQueryTemplate reads avro"""

        s3_to_bigquery_template = S3ToBigQueryTemplate()

        mock_parsed_args = s3_to_bigquery_template.parse_args(
            ["--s3.bq.input.location=s3a://bucket/file",
             "--s3.bq.access.key=SomeAccessKey",
             "--s3.bq.secret.key=SomeSecretKey",
             "--s3.bq.input.format=avro",
             "--s3.bq.output.dataset.name=xyz",
             "--s3.bq.output.table.name=lmn",
             "--s3.bq.temp.bucket.name=test",
             "--s3.bq.output.mode=overwrite"
             ])
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3ACCESSKEY, "SomeAccessKey")
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3SECRETKEY, "SomeSecretKey")
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3ENDPOINT, constants.S3_BQ_ENDPOINT_VALUE)

        mock_spark_session.read \
            .format() \
            .load \
            .return_value = mock_spark_session.dataframe.DataFrame
        s3_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format \
            .assert_called_with(constants.FORMAT_AVRO_EXTD)
        mock_spark_session.read \
            .format() \
            .load \
            .assert_called_with("s3a://bucket/file")

        mock_spark_session.dataframe.DataFrame.write \
            .format \
            .assert_called_once_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option \
            .assert_called_once_with(constants.TABLE, "xyz.lmn")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option \
            .assert_called_once_with(constants.TEMP_GCS_BUCKET, "test")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode \
            .assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args4(self, mock_spark_session):
        """Tests S3ToBigQueryTemplate reads csv"""

        s3_to_bigquery_template = S3ToBigQueryTemplate()

        mock_parsed_args = s3_to_bigquery_template.parse_args(
            ["--s3.bq.input.location=s3a://bucket/file",
             "--s3.bq.access.key=SomeAccessKey",
             "--s3.bq.secret.key=SomeSecretKey",
             "--s3.bq.input.format=csv",
             "--s3.bq.output.dataset.name=xyz",
             "--s3.bq.output.table.name=lmn",
             "--s3.bq.temp.bucket.name=test",
             "--s3.bq.output.mode=ignore"
             ])
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3ACCESSKEY, "SomeAccessKey")
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3SECRETKEY, "SomeSecretKey")
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3ENDPOINT, constants.S3_BQ_ENDPOINT_VALUE)

        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .load \
            .return_value = mock_spark_session.dataframe.DataFrame
        s3_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format \
            .assert_called_with(constants.FORMAT_CSV)
        mock_spark_session.read \
            .format() \
            .option \
            .assert_called_with('header',True)
        mock_spark_session.read \
            .format() \
            .option() \
            .option \
            .assert_called_with('inferSchema',True)
        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .load \
            .assert_called_with("s3a://bucket/file")

        mock_spark_session.dataframe.DataFrame.write \
            .format \
            .assert_called_once_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option \
            .assert_called_once_with(constants.TABLE, "xyz.lmn")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option \
            .assert_called_once_with(constants.TEMP_GCS_BUCKET, "test")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode \
            .assert_called_once_with(constants.OUTPUT_MODE_IGNORE)

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args5(self, mock_spark_session):
        """Tests S3ToBigQueryTemplate reads json"""

        s3_to_bigquery_template = S3ToBigQueryTemplate()

        mock_parsed_args = s3_to_bigquery_template.parse_args(
            ["--s3.bq.input.location=s3a://bucket/file",
             "--s3.bq.access.key=SomeAccessKey",
             "--s3.bq.secret.key=SomeSecretKey",
             "--s3.bq.input.format=json",
             "--s3.bq.output.dataset.name=xyz",
             "--s3.bq.output.table.name=lmn",
             "--s3.bq.temp.bucket.name=test",
             "--s3.bq.output.mode=errorifexists"
             ])
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3ACCESSKEY, "SomeAccessKey")
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3SECRETKEY, "SomeSecretKey")
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3ENDPOINT, constants.S3_BQ_ENDPOINT_VALUE)

        mock_spark_session.read \
            .json \
            .return_value = mock_spark_session.dataframe.DataFrame
        s3_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .json \
            .assert_called_with("s3a://bucket/file")

        mock_spark_session.dataframe.DataFrame.write \
            .format \
            .assert_called_once_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option \
            .assert_called_once_with(constants.TABLE, "xyz.lmn")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option \
            .assert_called_once_with(constants.TEMP_GCS_BUCKET, "test")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode \
            .assert_called_once_with(constants.OUTPUT_MODE_ERRORIFEXISTS)

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args6(self, mock_spark_session):
        """Tests S3ToBigQueryTemplate write mode defaults to append"""

        s3_to_bigquery_template = S3ToBigQueryTemplate()

        mock_parsed_args = s3_to_bigquery_template.parse_args(
            ["--s3.bq.input.location=s3a://bucket/file",
             "--s3.bq.access.key=SomeAccessKey",
             "--s3.bq.secret.key=SomeSecretKey",
             "--s3.bq.input.format=json",
             "--s3.bq.output.dataset.name=xyz",
             "--s3.bq.output.table.name=lmn",
             "--s3.bq.temp.bucket.name=test"
             ])
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3ACCESSKEY, "SomeAccessKey")
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3SECRETKEY, "SomeSecretKey")
        mock_spark_session._jsc.hadoopConfiguration() \
            .set(constants.AWS_S3ENDPOINT, constants.S3_BQ_ENDPOINT_VALUE)

        mock_spark_session.read \
            .json \
            .return_value = mock_spark_session.dataframe.DataFrame
        s3_to_bigquery_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .json \
            .assert_called_with("s3a://bucket/file")

        mock_spark_session.dataframe.DataFrame.write \
            .format \
            .assert_called_once_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option \
            .assert_called_once_with(constants.TABLE, "xyz.lmn")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option \
            .assert_called_once_with(constants.TEMP_GCS_BUCKET, "test")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode \
            .assert_called_once_with(constants.OUTPUT_MODE_APPEND)
