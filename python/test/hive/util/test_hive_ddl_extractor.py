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
from dataproc_templates.hive.util.hive_ddl_extractor import \
    HiveDDLExtractorTemplate
from datetime import datetime

class TestHiveDDLExtractorTemplate:
    """
    Test suite for HiveDDLExtractorTemplate
    """

    def test_parse_args(self):
        """Tests HiveDDLExtractorTemplate.parse_args()"""

        hive_ddl_extractor_template = HiveDDLExtractorTemplate()
        parsed_args = hive_ddl_extractor_template.parse_args(
            ["--hive.ddl.extractor.input.database=database",
             "--hive.ddl.extractor.output.path=gs://bucket/path"])

        assert parsed_args["hive.ddl.extractor.input.database"] == "database"
        assert parsed_args["hive.ddl.extractor.output.path"] == "gs://bucket/path"

    @mock.patch.object(pyspark.sql, 'SparkSession')
    def test_run(self, mock_spark_session):
        """Tests HiveDDLExtractorTemplate runs with append mode"""

        hive_ddl_extractor_template = HiveDDLExtractorTemplate()
        mock_parsed_args = hive_ddl_extractor_template.parse_args(
            ["--hive.ddl.extractor.input.database=database",
             "--hive.ddl.extractor.output.path=gs://bucket/path"])
        mock_spark_session.sql.return_value=mock_spark_session.dataframe.DataFrame
        mock_spark_session.sparkContext.parallelize.return_value=mock_spark_session.rdd.RDD
        hive_ddl_extractor_template.run(mock_spark_session, mock_parsed_args)
        mock_spark_session.sql.assert_called_once_with("SHOW TABLES IN database")
        mock_spark_session.sparkContext.parallelize.assert_called_once_with([])
        ct = datetime.now().strftime("%m-%d-%Y %H.%M.%S")
        mock_spark_session.rdd.RDD.coalesce().saveAsTextFile.assert_called_once_with("gs://bucket/path/database/{ct}".format(ct=ct))