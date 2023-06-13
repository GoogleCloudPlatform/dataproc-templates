# Copyright 2023 Google LLC
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
from typing import Dict, Sequence, Optional, Any
import argparse
import json

import papermill as pm

from parameterize_script import BaseParameterizeScript
import parameterize_script.util.notebook_constants as constants
from parameterize_script.util import get_common_args


__all__ = ['PostgreSqlToBigQueryScript']

class PostgreSqlToBigQueryScript(BaseParameterizeScript):

    """
    Script to parameterize PostgreSql to Big Query notebook.
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.POSTGRESQL_HOST_ARG}',
            dest=constants.POSTGRESQL_HOST,
            required=True,
            help='POSTGRESQL host or IP address'
        )

        parser.add_argument(
            f'--{constants.POSTGRESQL_PORT_ARG}',
            dest=constants.POSTGRESQL_PORT,
            default="3306",
            required=False,
            help='POSTGRESQL port (Default: 3306)'
        )

        parser.add_argument(
            f'--{constants.POSTGRESQL_USERNAME_ARG}',
            dest=constants.POSTGRESQL_USERNAME,
            required=True,
            help='POSTGRESQL username'
        )

        parser.add_argument(
            f'--{constants.POSTGRESQL_PASSWORD_ARG}',
            dest=constants.POSTGRESQL_PASSWORD,
            required=True,
            help='POSTGRESQL password'
        )

        parser.add_argument(
            f'--{constants.POSTGRESQL_DATABASE_ARG}',
            dest=constants.POSTGRESQL_DATABASE,
            required=True,
            help='POSTGRESQL database name'
        )

        parser.add_argument(
            f'--{constants.POSTGRESQL_TABLE_LIST_ARG}',
            dest=constants.POSTGRESQL_TABLE_LIST,
            required=False,
            default='',
            help='POSTGRESQL table list to migrate. '
            'Leave empty for migrating complete database else provide tables as \"table1,table2\"'
        )

        parser.add_argument(
            f'--{constants.POSTGRESQL_SCHEMA_LIST_ARG}',
            dest=constants.POSTGRESQL_SCHEMA_LIST,
            required=False,
            default='',
            help='POSTGRESQL schema list to migrate. '
            'Only Migrate tables associated with the provided schema list. Provide schema as \"schema1,schema2\" '
        )

       
        parser.add_argument(
            f'--{constants. BIGQUERY_DATASET_ARG}',
            dest=constants. BIGQUERY_DATASET,
            required=True,
            help=' BIGQUERY dataset name'
        )

        parser.add_argument(
            f'--{constants. BIGQUERY_MODE_ARG}',
            dest=constants. BIGQUERY_MODE,
            default='overwrite',
            required=True,
            help='output write mode (Default: overwrite)'
        )

        parser.add_argument(
            f'--{constants.MAX_PARALLELISM_ARG}',
            dest=constants.MAX_PARALLELISM,
            type=int,
            default=5,
            required=False,
            help='Maximum number of tables that will migrated parallelly (Default: 5)'
        )

        parser = get_common_args(parser)
        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args()
        return vars(known_args)


    def run(self, args: Dict[str, Any]) -> None:
        """
        Run the notebook.
        """

        # Convert comma separated string to list
        args[constants.POSTGRESQL_TABLE_LIST] = list(
            map(str.strip, args[constants.POSTGRESQL_TABLE_LIST].split(","))
        )
        args[constants.POSTGRESQL_SCHEMA_LIST] = list(
            map(str.strip, args[constants.POSTGRESQL_SCHEMA_LIST].split(","))
        )
        # Exclude arguments that are not needed to be passed to the notebook
        ignore_keys = {constants.LOG_LEVEL_ARG, constants.OUTPUT_NOTEBOOK_ARG}
        nb_parameters = {key:val for key,val in args.items() if key not in ignore_keys}

        # Get environment variables
        env_vars = PostgreSqlToBigQueryScript.get_env_vars()
        nb_parameters.update(env_vars)


        # Run the notebook
        output_path = args[constants.OUTPUT_NOTEBOOK_ARG]
        pm.execute_notebook(
            'postgresql2bq/postgresql-to-bigquery-notebook.ipynb',
            output_path,
            nb_parameters,
            log_output=True

        )