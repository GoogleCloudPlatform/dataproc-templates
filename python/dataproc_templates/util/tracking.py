# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dataproc_templates import TemplateName

import google.auth
from google.api_core import client_info as http_client_info
from google.cloud import bigquery

def track_template_invocation(template_name: TemplateName) -> None:
    """
    Track template invocation

    Args:
        template_name (TemplateName): The TemplateName of the template
            class being run.

    Returns:
        None
    """

    project_id: str
    _, project_id = google.auth.default()

    try:
        client_info = http_client_info.ClientInfo(user_agent=f"google-pso-tool/dataproc-templates/0.1.0-{template_name.value}")
        client = bigquery.Client(project=project_id, client_info=client_info)
        dataset_list = client.list_datasets(
            project='bigquery-public-data',
            page_size=1
        )
    except Exception:
        # Do nothing
        pass