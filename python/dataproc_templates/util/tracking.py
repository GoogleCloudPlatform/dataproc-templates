
from typing import Dict

import google.auth
from google.auth.credentials import Credentials
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery

from dataproc_templates import TemplateName


def _build_tracking_request_headers(
    template_name: TemplateName
) -> Dict[str, str]:
    return {
        'user-agent': f"google-pso-tool/dataproc-templates/0.1.0-{template_name.value}"
    }


def track_template_invocation(template_name: TemplateName) -> None:
    """
    Track template invocation

    Args:
        template_name (TemplateName): The TemplateName of the template
            class being run.

    Returns:
        None
    """

    # pylint: disable=broad-except

    credentials: Credentials
    project_id: str
    credentials, project_id = google.auth.default()

    headers: Dict[str, str] = _build_tracking_request_headers(
        template_name=template_name
    )

    authorized_session: AuthorizedSession = AuthorizedSession(
        credentials=credentials
    )

    authorized_session.headers.update(headers)

    try:
        with bigquery.Client(project=project_id, _http=authorized_session) as client:
            client.list_datasets(
                project='bigquery-public-data',
                page_size=1
            )
    except Exception:
        # Do nothing
        pass
