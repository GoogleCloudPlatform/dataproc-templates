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

from dataproc_templates.mongo.mongo_to_bq import MongoToBigQueryTemplate
import dataproc_templates.util.template_constants as constants


class TestMongoToBigQueryTemplate:
    """
    Test suite for MongoToBigQueryTemplate
    """

    def test_parse_args(self):
        """Tests MongoToBigQueryTemplate.parse_args()"""

        mongo_to_gcs_template = MongoToBigQueryTemplate()
        parsed_args = mongo_to_gcs_template.parse_args(
            ["--mongo.bq.input.uri=mongodb://host:port",
             "--mongo.bq.input.database=database",
             "--mongo.bq.input.collection=collection",
             "--mongo.bq.output.dataset=dataset",
             "--mongo.bq.output.table=table",
             "--mongo.bq.output.mode=overwrite",
             "--mongo.bq.temp.bucket.name=bucket"])

        assert parsed_args["mongo.bq.input.uri"] == "mongodb://host:port"
        assert parsed_args["mongo.bq.input.database"] == "database"
        assert parsed_args["mongo.bq.input.collection"] == "collection"
        assert parsed_args["mongo.bq.output.dataset"] == "dataset"
        assert parsed_args["mongo.bq.output.table"] == "table"
        assert parsed_args["mongo.bq.output.mode"] == "overwrite"
        assert parsed_args["mongo.bq.temp.bucket.name"] == "bucket"