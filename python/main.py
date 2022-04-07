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

from typing import Dict, Type
import logging
import sys

from dataproc_templates import BaseTemplate, TemplateName
from dataproc_templates.util import get_template_name
from dataproc_templates.gcs.gcs_to_bigquery import GcsToBigQueryTemplate
from dataproc_templates.bigquery.bigquery_to_gcs import BigQueryToGCSTemplate


LOGGER: logging.Logger = logging.getLogger('dataproc_templates')


# Maps each TemplateName to its corresponding implementation
# of BaseTemplate
TEMPLATE_IMPLS: Dict[TemplateName, Type[BaseTemplate]] = {
    TemplateName.GCSTOBIGQUERY: GcsToBigQueryTemplate,
    TemplateName.BIGQUERYTOGCS: BigQueryToGCSTemplate
}


def get_template_impl(template_name: str) -> Type[BaseTemplate]:
    """
    Gets the corresponding template implementation class given
    it's template name.

    Args:
        template_name (str): The name of the template for which
            to get the implementation class.

    Returns:
        Type[BaseTemplate]: The class that implements the corresponding
            template.
    
    Raises:
        ValueError: if the given template name is invalid
    """

    parsed_template_name: TemplateName = \
        TemplateName.from_string(template_name)
    return TEMPLATE_IMPLS[parsed_template_name]


def run_template(template_name: str) -> None:
    """
    Executes a template given it's template name.
    
    Args:
        template_name (str): The name of the template that
            should be run.
    
    Returns:
        None
    
    Raises:
        ValueError: if the given template name is invalid
    """

    template_impl: Type[BaseTemplate] = get_template_impl(
        template_name=template_name
    )

    LOGGER.info(f'Running template {template_name}')

    template_instance: BaseTemplate = template_impl.build()

    try:
        template_instance.run()
    except Exception:
        LOGGER.exception(
            f'An error occurred while running {template_name} template'
        )
        sys.exit(1)


if __name__ == '__main__':
    LOGGER.setLevel(logging.INFO)

    run_template(
        template_name=get_template_name()
    )
