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

from dataproc_templates.pubsublite.pubsublite_to_bigquery import PubsubliteToBQTemplate
import dataproc_templates.util.template_constants as constants


class TestPubsubliteToBQTemplate:
    """
    Test suite for PubsubliteToBQTemplate
    """

    def test_parse_args1(self):
        """Tests PubsubliteToBQTemplate.parse_args()"""

        pubsublite_to_bq_template = PubsubliteToBQTemplate()
        parsed_args = pubsublite_to_bq_template.parse_args(
            ["--pubsublite.to.bq.input.subscription.url=url",
             "--pubsublite.to.bq.project.id=projectID",
             "--pubsublite.to.bq.output.dataset=dataset1",
             "--pubsublite.to.bq.output.table=table1",
             "--pubsublite.to.bq.write.mode=overwrite",
             "--pubsublite.to.bq.temp.bucket.name=bucket",
             "--pubsublite.to.bq.checkpoint.location=gs://test"
             ])       
        
        assert parsed_args["pubsublite.to.bq.input.subscription.url"] == "url"
        assert parsed_args["pubsublite.to.bq.project.id"] == "projectID"
        assert parsed_args["pubsublite.to.bq.output.dataset"] == "dataset1"
        assert parsed_args["pubsublite.to.bq.output.table"] == "table1"
        assert parsed_args["pubsublite.to.bq.write.mode"] == "overwrite"  
        assert parsed_args["pubsublite.to.bq.temp.bucket.name"] == "bucket"   
        assert parsed_args["pubsublite.to.bq.checkpoint.location"] == "gs://test"   