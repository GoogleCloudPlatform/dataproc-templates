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
from dataproc_templates.cassandra.cassandra_to_bigquery import CassandraToBQTemplate
import dataproc_templates.util.template_constants as constants


class TestCassandraToBQTemplate:
    """
    Test suite for CassandraToBQTemplate
    """

    def test_parse_args1(self):
        """Tests CassandraToBQTemplate.parse_args()"""

        cassandra_to_bq_template = CassandraToBQTemplate()
        parsed_args = cassandra_to_bq_template.parse_args(
            ["--cassandratobq.input.table=tablename",
             "--cassandratobq.input.host=192.168.2.2",
             "--cassandratobq.bigquery.location=dataset.table",
             "--cassandratobq.output.mode=append",
             "--cassandratobq.temp.gcs.location=xyz",
             "--cassandratobq.input.query=select one from sample",
             "--cassandratobq.input.catalog.name=casscon",
             "--cassandratobq.input.keyspace=tk1"
             ])

        assert parsed_args["cassandratobq.input.table"] == "tablename"
        assert parsed_args["cassandratobq.input.host"] == "192.168.2.2"
        assert parsed_args["cassandratobq.bigquery.location"] == "dataset.table"
        assert parsed_args["cassandratobq.output.mode"] == "append"
        assert parsed_args["cassandratobq.temp.gcs.location"] == "xyz"
        assert parsed_args["cassandratobq.input.query"] == "select one from sample"
        assert parsed_args["cassandratobq.input.catalog.name"] == "casscon"
        assert parsed_args["cassandratobq.input.keyspace"] == "tk1"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run_pass_args2(self, mock_spark_session):
        """Tests CassandraToBQTemplate write"""

        cassandra_to_bq_template = CassandraToBQTemplate()

        mock_parsed_args = cassandra_to_bq_template.parse_args(
            ["--cassandratobq.input.table=tablename",
             "--cassandratobq.input.host=192.168.2.2",
             "--cassandratobq.bigquery.location=dataset.table",
             "--cassandratobq.output.mode=append",
             "--cassandratobq.temp.gcs.location=xyz",
             "--cassandratobq.input.query=select one from sample",
             "--cassandratobq.input.catalog.name=casscon",
             "--cassandratobq.input.keyspace=tk1"
             ])
        mock_spark_session.table.return_value = mock_spark_session.dataframe.DataFrame
        cassandra_to_bq_template.run(mock_spark_session, mock_parsed_args)

        mock_spark_session.read \
            .format() \
            .option() \
            .option() \
            .option() \
            .load.assert_called_with()

        mock_spark_session.dataframe.DataFrame.write \
            .format.assert_called_once_with(constants.FORMAT_BIGQUERY)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option.assert_called_once_with(constants.TABLE, "dataset.table")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option.assert_called_once_with(constants.TEMP_GCS_BUCKET, "xyz")
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode.assert_called_once_with(constants.OUTPUT_MODE_APPEND)
        mock_spark_session.dataframe.DataFrame.write \
            .format() \
            .option() \
            .option() \
            .mode() \
            .save.assert_called_once()
