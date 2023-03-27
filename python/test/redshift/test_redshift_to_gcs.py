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

from dataproc_templates.redshift.redshift_to_gcs import RedshiftToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestRedshiftToGCSTemplate:
    """
    Test suite for RedshiftToGCSTemplate
    """

    def test_parse_args1(self):
        """Tests RedshiftToGCSTemplate.parse_args()"""

        redshift_to_gcs_template = RedshiftToGCSTemplate()
        parsed_args = redshift_to_gcs_template.parse_args(
            ["--redshifttogcs.input.url=url",
             "--redshifttogcs.s3.tempdir=s3a://temp",
             "--redshifttogcs.input.table=table1",
             "--redshifttogcs.iam.rolearn=arn:aws:iam::xxxx:role/role",
             "--redshifttogcs.s3.accesskey=xyz",
             "--redshifttogcs.s3.secretkey=lmn",
             "--redshifttogcs.output.location=gs://test",
             "--redshifttogcs.output.format=csv",
             "--redshifttogcs.output.mode=append",
             "--redshifttogcs.output.partitioncolumn=column"
             ])

        assert parsed_args["redshifttogcs.input.url"] == "url"
        assert parsed_args["redshifttogcs.s3.tempdir"] == "s3a://temp"
        assert parsed_args["redshifttogcs.input.table"] == "table1"
        assert parsed_args["redshifttogcs.iam.rolearn"] == "arn:aws:iam::xxxx:role/role"
        assert parsed_args["redshifttogcs.s3.accesskey"] == "xyz"
        assert parsed_args["redshifttogcs.s3.secretkey"] == "lmn"
        assert parsed_args["redshifttogcs.output.location"] == "gs://test"
        assert parsed_args["redshifttogcs.output.format"] == "csv"
        assert parsed_args["redshifttogcs.output.mode"] == "append"
        assert parsed_args["redshifttogcs.output.partitioncolumn"] == "column"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args2(self, mock_spark_session):
        """Tests RedshiftToGCSTemplate write parquet"""

        redshift_to_gcs_template = RedshiftToGCSTemplate()

        mock_parsed_args = redshift_to_gcs_template.parse_args(
            ["--redshifttogcs.input.url=url",
             "--redshifttogcs.s3.tempdir=s3a://temp",
             "--redshifttogcs.input.table=table1",
             "--redshifttogcs.iam.rolearn=arn:aws:iam::xxxx:role/role",
             "--redshifttogcs.s3.accesskey=xyz",
             "--redshifttogcs.s3.secretkey=lmn",
             "--redshifttogcs.output.location=gs://test",
             "--redshifttogcs.output.format=parquet",
             "--redshifttogcs.output.mode=overwrite"
             ])
        mock_spark_session._jsc.hadoopConfiguration().set(constants.AWS_S3ACCESSKEY, "xyz")
        mock_spark_session._jsc.hadoopConfiguration().set(constants.AWS_S3SECRETKEY, "lmn")
        mock_spark_session.read.format().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        redshift_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_REDSHIFT)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.REDSHIFT_TEMPDIR, "s3a://temp")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.REDSHIFT_IAMROLE, "arn:aws:iam::xxxx:role/role")
        mock_spark_session.read.format().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_OVERWRITE)
        mock_spark_session.dataframe.DataFrame.write.mode().parquet.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args3(self, mock_spark_session):
        """Tests RedshiftToGCSTemplate write avro"""

        redshift_to_gcs_template = RedshiftToGCSTemplate()

        mock_parsed_args = redshift_to_gcs_template.parse_args(
            ["--redshifttogcs.input.url=url",
             "--redshifttogcs.s3.tempdir=s3a://temp",
             "--redshifttogcs.input.table=table1",
             "--redshifttogcs.iam.rolearn=arn:aws:iam::xxxx:role/role",
             "--redshifttogcs.s3.accesskey=xyz",
             "--redshifttogcs.s3.secretkey=lmn",
             "--redshifttogcs.output.location=gs://test",
             "--redshifttogcs.output.format=avro",
             "--redshifttogcs.output.mode=append"
             ])
        mock_spark_session.read.format().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        redshift_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_REDSHIFT)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.REDSHIFT_TEMPDIR, "s3a://temp")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.REDSHIFT_IAMROLE, "arn:aws:iam::xxxx:role/role")
        mock_spark_session.read.format().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().format.assert_called_once_with(constants.FORMAT_AVRO)
        mock_spark_session.dataframe.DataFrame.write.mode().format().save.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args4(self, mock_spark_session):
        """Tests RedshiftToGCSTemplate write csv"""

        redshift_to_gcs_template = RedshiftToGCSTemplate()

        mock_parsed_args = redshift_to_gcs_template.parse_args(
            ["--redshifttogcs.input.url=url",
             "--redshifttogcs.s3.tempdir=s3a://temp",
             "--redshifttogcs.input.table=table1",
             "--redshifttogcs.iam.rolearn=arn:aws:iam::xxxx:role/role",
             "--redshifttogcs.s3.accesskey=xyz",
             "--redshifttogcs.s3.secretkey=lmn",
             "--redshifttogcs.output.location=gs://test",
             "--redshifttogcs.output.format=csv",
             "--redshifttogcs.output.mode=ignore"
             ])
        mock_spark_session.read.format().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        redshift_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_REDSHIFT)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.REDSHIFT_TEMPDIR, "s3a://temp")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.REDSHIFT_IAMROLE, "arn:aws:iam::xxxx:role/role")
        mock_spark_session.read.format().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_IGNORE)
        mock_spark_session.dataframe.DataFrame.write.mode().options.assert_called_once_with(**{constants.CSV_HEADER: 'true'})
        mock_spark_session.dataframe.DataFrame.write.mode().options().csv.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args5(self, mock_spark_session):
        """Tests RedshiftToGCSTemplate write json"""

        redshift_to_gcs_template = RedshiftToGCSTemplate()

        mock_parsed_args = redshift_to_gcs_template.parse_args(
            ["--redshifttogcs.input.url=url",
             "--redshifttogcs.s3.tempdir=s3a://temp",
             "--redshifttogcs.input.table=table1",
             "--redshifttogcs.iam.rolearn=arn:aws:iam::xxxx:role/role",
             "--redshifttogcs.s3.accesskey=xyz",
             "--redshifttogcs.s3.secretkey=lmn",
             "--redshifttogcs.output.location=gs://test",
             "--redshifttogcs.output.format=json",
             "--redshifttogcs.output.mode=ignore"
             ])
        mock_spark_session.read.format().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        redshift_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_REDSHIFT)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.REDSHIFT_TEMPDIR, "s3a://temp")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.REDSHIFT_IAMROLE, "arn:aws:iam::xxxx:role/role")
        mock_spark_session.read.format().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_IGNORE)
        mock_spark_session.dataframe.DataFrame.write.mode().json.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args6(self, mock_spark_session):
        """Tests RedshiftToGCSTemplate pass args"""

        redshift_to_gcs_template = RedshiftToGCSTemplate()

        mock_parsed_args = redshift_to_gcs_template.parse_args(
            ["--redshifttogcs.input.url=url",
             "--redshifttogcs.s3.tempdir=s3a://temp",
             "--redshifttogcs.input.table=table1",
             "--redshifttogcs.iam.rolearn=arn:aws:iam::xxxx:role/role",
             "--redshifttogcs.s3.accesskey=xyz",
             "--redshifttogcs.s3.secretkey=lmn",
             "--redshifttogcs.output.location=gs://test",
             "--redshifttogcs.output.format=csv",
             "--redshifttogcs.output.mode=append"
             ])
        mock_spark_session.read.format().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        redshift_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_REDSHIFT)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.REDSHIFT_TEMPDIR, "s3a://temp")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.REDSHIFT_IAMROLE, "arn:aws:iam::xxxx:role/role")
        mock_spark_session.read.format().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().options.assert_called_once_with(**{constants.CSV_HEADER: 'true'})
        mock_spark_session.dataframe.DataFrame.write.mode().options().csv.assert_called_once_with("gs://test")

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args7(self, mock_spark_session):
        """Tests RedshiftToGCSTemplate pass args"""

        redshift_to_gcs_template = RedshiftToGCSTemplate()

        mock_parsed_args = redshift_to_gcs_template.parse_args(
            ["--redshifttogcs.input.url=url",
             "--redshifttogcs.s3.tempdir=s3a://temp",
             "--redshifttogcs.input.table=table1",
             "--redshifttogcs.iam.rolearn=arn:aws:iam::xxxx:role/role",
             "--redshifttogcs.s3.accesskey=xyz",
             "--redshifttogcs.s3.secretkey=lmn",
             "--redshifttogcs.output.location=gs://test",
             "--redshifttogcs.output.format=csv",
             "--redshifttogcs.output.mode=append",
             "--redshifttogcs.output.partitioncolumn=column"
             ])
        mock_spark_session.read.format().option().option().option().option().load.return_value = mock_spark_session.dataframe.DataFrame
        redshift_to_gcs_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.read.format.assert_called_with(constants.FORMAT_REDSHIFT)
        mock_spark_session.read.format().option.assert_called_with(constants.JDBC_URL, "url")
        mock_spark_session.read.format().option().option.assert_called_with(constants.JDBC_TABLE, "table1")
        mock_spark_session.read.format().option().option().option.assert_called_with(constants.REDSHIFT_TEMPDIR, "s3a://temp")
        mock_spark_session.read.format().option().option().option().option.assert_called_with(constants.REDSHIFT_IAMROLE, "arn:aws:iam::xxxx:role/role")
        mock_spark_session.read.format().option().option().option().option().load()
        mock_spark_session.dataframe.DataFrame.write.mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy.assert_called_once_with("column")
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy().options.assert_called_once_with(**{constants.CSV_HEADER: 'true'})
        mock_spark_session.dataframe.DataFrame.write.mode().partitionBy().options().csv.assert_called_once_with("gs://test")
