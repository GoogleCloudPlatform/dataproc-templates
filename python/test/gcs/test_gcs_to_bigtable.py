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

from dataproc_templates.gcs.gcs_to_bigtable import GCSToBigTableTemplate
import dataproc_templates.util.template_constants as constants


class TestGCSToBigTableTemplate:
    """
    Test suite for GCSToBigTableTemplate
    """

    def test_parse_args(self):
        """Tests GCSToBigTableTemplate.parse_args()"""

        gcs_to_bigtable_template = GCSToBigTableTemplate()
        parsed_args = gcs_to_bigtable_template.parse_args(
            ["--gcs.bigtable.input.format=parquet",
             "--gcs.bigtable.input.location=gs://test",
             "--spark.bigtable.project.id=GCP_PROJECT",
             "--spark.bigtable.instance.id=BIGTABLE_INSTANCE_ID",
             "--gcs.bigtable.catalog.json=gs://dataproc-templates/conf/employeecatalog.json"])

        assert parsed_args["gcs.bigtable.input.format"] == "parquet"
        assert parsed_args["gcs.bigtable.input.location"] == "gs://test"
        assert parsed_args["spark.bigtable.project.id"] == "GCP_PROJECT"
        assert parsed_args["spark.bigtable.instance.id"] == "BIGTABLE_INSTANCE_ID"
        assert parsed_args["gcs.bigtable.catalog.json"] == 'gs://dataproc-templates/conf/employeecatalog.json'
