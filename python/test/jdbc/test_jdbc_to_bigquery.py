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


from dataproc_templates.jdbc.jdbc_to_bigquery import JDBCToBigQueryTemplate
import dataproc_templates.util.template_constants as constants


class TestJDBCToGCSTemplate:
    """
    Test suite for JDBCToBigQueryTemplate
    """

    def test_parse_args1(self):
        """Tests JDBCToBigQueryTemplate.parse_args()"""

        jdbc_to_bigquery_template = JDBCToBigQueryTemplate()
        parsed_args = jdbc_to_bigquery_template.parse_args(
            ["--jdbc.bigquery.input.url=url",
             "--jdbc.bigquery.input.driver=driver",
             "--jdbc.bigquery.input.table=table1",
             "--jdbc.bigquery.input.partitioncolumn=column",
             "--jdbc.bigquery.input.lowerbound=1",
             "--jdbc.bigquery.input.upperbound=2",
             "--jdbc.bigquery.numpartitions=5",
             "--jdbc.bigquery.output.mode=append",
             "--jdbc.bigquery.output.dataset=bq-dataset",
             "--jdbc.bigquery.output.table=bq-table",
             "--jdbc.bigquery.temp.bucket.name=bucket-name",
             ])       
        
        assert parsed_args["jdbc.bigquery.input.url"] == "url"
        assert parsed_args["jdbc.bigquery.input.driver"] == "driver"
        assert parsed_args["jdbc.bigquery.input.table"] == "table1"
        assert parsed_args["jdbc.bigquery.input.partitioncolumn"] == "column"
        assert parsed_args["jdbc.bigquery.input.lowerbound"] == "1"
        assert parsed_args["jdbc.bigquery.input.upperbound"] == "2"
        assert parsed_args["jdbc.bigquery.numpartitions"] == "5"
        assert parsed_args["jdbc.bigquery.output.mode"] == "append"  
        assert parsed_args["jdbc.bigquery.output.dataset"] == "bq-dataset"  
        assert parsed_args["jdbc.bigquery.output.table"] == "bq-table"  
        assert parsed_args["jdbc.bigquery.temp.bucket.name"] == "bucket-name"  

    def test_run_pass_args2(self, mock_spark_session):
        """Tests JDBCToBigQueryTemplate pass args"""

        jdbc_to_bigquery_template = JDBCToBigQueryTemplate()

        mock_parsed_args = jdbc_to_bigquery_template.parse_args(
            ["--jdbctogcs.input.url=url",
             "--jdbctogcs.input.driver=driver",
             "--jdbctogcs.input.table=table1",
             "--jdbctogcs.output.mode=append",
             "--jdbc.bigquery.output.dataset=bq-dataset",
             "--jdbc.bigquery.output.table=bq-table",
             "--jdbc.bigquery.temp.bucket.name=bucket-name",
             ])

        assert parsed_args["jdbc.bigquery.input.url"] == "url"
        assert parsed_args["jdbc.bigquery.input.driver"] == "driver"
        assert parsed_args["jdbc.bigquery.input.table"] == "table1"
        assert parsed_args["jdbc.bigquery.output.mode"] == "append"  
        assert parsed_args["jdbc.bigquery.output.dataset"] == "bq-dataset"  
        assert parsed_args["jdbc.bigquery.output.table"] == "bq-table"  
        assert parsed_args["jdbc.bigquery.temp.bucket.name"] == "bucket-name"  
        
