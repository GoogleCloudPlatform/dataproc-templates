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
from dataproc_templates.pubsublite.pubsublite_to_gcs import PubSubLiteToGCSTemplate
import dataproc_templates.util.template_constants as constants


class TestPubSubLiteToGCSTemplate:
    """
    Test suite for PubSubLiteToGCSTemplate
    """

    def test_parse_args1(self):
        """Tests PubSubLiteToGCSTemplate.parse_args()"""

        pubsublite_to_gcs_template = PubSubLiteToGCSTemplate()
        parsed_args = pubsublite_to_gcs_template.parse_args(
            ["--pubsublite.to.gcs.input.subscription.url=test-sub",
             "--pubsublite.to.gcs.write.mode=append",
             "--pubsublite.to.gcs.output.location=gs://test",
             "--pubsublite.to.gcs.checkpoint.location=gs://test-checkpoint",
             "--pubsublite.to.gcs.output.format=json",
             "--pubsublite.to.gcs.timeout=120",
             "--pubsublite.to.gcs.processing.time=1"
             ])

        assert parsed_args["pubsublite.to.gcs.input.subscription.url"] == "test-sub"
        assert parsed_args["pubsublite.to.gcs.write.mode"] == "append"
        assert parsed_args["pubsublite.to.gcs.output.location"] == "gs://test"
        assert parsed_args["pubsublite.to.gcs.checkpoint.location"] == "gs://test-checkpoint"
        assert parsed_args["pubsublite.to.gcs.output.format"] == "json"
        assert parsed_args["pubsublite.to.gcs.timeout"] == 120
        assert parsed_args["pubsublite.to.gcs.processing.time"] == "1"

    def test_parse_args2(self):
        """Tests PubSubLiteToGCSTemplate.parse_args() when output format not passed"""

        pubsublite_to_gcs_template = PubSubLiteToGCSTemplate()
        parsed_args = pubsublite_to_gcs_template.parse_args(
            ["--pubsublite.to.gcs.input.subscription.url=test-sub",
             "--pubsublite.to.gcs.write.mode=append",
             "--pubsublite.to.gcs.output.location=gs://test",
             "--pubsublite.to.gcs.checkpoint.location=gs://test-checkpoint",
             "--pubsublite.to.gcs.timeout=120",
             "--pubsublite.to.gcs.processing.time=1"
             ])

        assert parsed_args["pubsublite.to.gcs.input.subscription.url"] == "test-sub"
        assert parsed_args["pubsublite.to.gcs.write.mode"] == "append"
        assert parsed_args["pubsublite.to.gcs.output.location"] == "gs://test"
        assert parsed_args["pubsublite.to.gcs.checkpoint.location"] == "gs://test-checkpoint"
        assert parsed_args["pubsublite.to.gcs.output.format"] == "json"
        assert parsed_args["pubsublite.to.gcs.timeout"] == 120
        assert parsed_args["pubsublite.to.gcs.processing.time"] == "1"

    def test_parse_args3(self):
        """Tests PubSubLiteToGCSTemplate.parse_args() when output mode not passed"""

        pubsublite_to_gcs_template = PubSubLiteToGCSTemplate()
        parsed_args = pubsublite_to_gcs_template.parse_args(
            ["--pubsublite.to.gcs.input.subscription.url=test-sub",
             "--pubsublite.to.gcs.output.location=gs://test",
             "--pubsublite.to.gcs.checkpoint.location=gs://test-checkpoint",
             "--pubsublite.to.gcs.output.format=json",
             "--pubsublite.to.gcs.timeout=120",
             "--pubsublite.to.gcs.processing.time=1"
             ])

        assert parsed_args["pubsublite.to.gcs.input.subscription.url"] == "test-sub"
        assert parsed_args["pubsublite.to.gcs.write.mode"] == "append"
        assert parsed_args["pubsublite.to.gcs.output.location"] == "gs://test"
        assert parsed_args["pubsublite.to.gcs.checkpoint.location"] == "gs://test-checkpoint"
        assert parsed_args["pubsublite.to.gcs.output.format"] == "json"
        assert parsed_args["pubsublite.to.gcs.timeout"] == 120
        assert parsed_args["pubsublite.to.gcs.processing.time"] == "1"
