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

import pytest
from airflow.models import DagBag

@pytest.fixture()
def dagbag():
  return DagBag(include_examples=False)

def test_expected_dags(dagbag):

  dag_ids = dagbag.dag_ids
  expected = ['submit_spark_dataproc_template', 'submit_pyspark_dataproc_template']

  assert all([expected_id in dag_ids for expected_id in expected])

def test_no_import_errors(dagbag):

  assert not dagbag.import_errors