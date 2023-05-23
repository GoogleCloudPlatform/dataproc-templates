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
from dataproc_templates.cassandra.cassandra_to_gcs import CassandraToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestCassandraToGCSTemplate:
    """
    Test suite for CassandraToGCSTemplate
    """

    def test_parse_args1(self):
        """Tests CassandraToGCSTemplate.parse_args()"""

        cassandra_to_gcs_template = CassandraToGCSTemplate()
        parsed_args = cassandra_to_gcs_template.parse_args(
            ["--cassandratogcs.input.table=tablename",
             "--cassandratogcs.input.host=192.168.2.2",
             "--cassandratogcs.output.format=csv",
             "--cassandratogcs.output.path=dataset.table",
             "--cassandratogcs.output.savemode=append",
             "--cassandratogcs.input.catalog.name=casscon",
             "--cassandratogcs.input.keyspace=tk1"
             ])

        assert parsed_args["cassandratogcs.input.table"] == "tablename"
        assert parsed_args["cassandratogcs.input.host"] == "192.168.2.2"
        assert parsed_args["cassandratogcs.output.format"] == "csv"
        assert parsed_args["cassandratogcs.output.path"] == "dataset.table"
        assert parsed_args["cassandratogcs.output.savemode"] == "append"
        assert parsed_args["cassandratogcs.input.catalog.name"] == "casscon"
        assert parsed_args["cassandratogcs.input.keyspace"] == "tk1"