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


from dataproc_templates.gcs.gcs_to_spanner import GCSToSpannerTemplate


class TestGCSToSpannerTemplate:
  """
  Test suite for GCSToBigQueryTemplate
  """

  def test_parse_args(self):
    """Tests GCSToSpannerTemplate.parse_args()"""

    gcs_to_spanner_template = GCSToSpannerTemplate()
    parsed_args = gcs_to_spanner_template.parse_args(
      [
        "--gcs.spanner.input.location=gs://test",
        "--gcs.spanner.input.format=parquet",
        "--gcs.spanner.output.instance=instance",
        "--gcs.spanner.output.database=database",
        "--gcs.spanner.output.table=table",
        "--gcs.spanner.output.primary_key=primary_key",
        "--gcs.spanner.output.mode=append",
        "--gcs.spanner.output.batch_size=300",
      ]
    )
    assert parsed_args["gcs.spanner.input.location"] == "gs://test"
    assert parsed_args["gcs.spanner.input.format"] == "parquet"
    assert parsed_args["gcs.spanner.output.instance"] == "instance"
    assert parsed_args["gcs.spanner.output.database"] == "database"
    assert parsed_args["gcs.spanner.output.table"] == "table"
    assert parsed_args["gcs.spanner.output.primary_key"] == "primary_key"
    assert parsed_args["gcs.spanner.output.mode"] == "append"
    assert parsed_args["gcs.spanner.output.batch_size"] == "300"
