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

from os import environ
from typing import Dict, Sequence, Optional, Any
import argparse
import json

import papermill as pm

from parameterize_script import BaseParameterizeScript
import parameterize_script.util.notebook_constants as constants

__all__ = ['MySqlToSpannerScript']

class MySqlToSpannerScript(BaseParameterizeScript):

    """
    Script to parameterize MySqlToSpanner notebook.
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.OUTPUT_NOTEBOOK_ARG}',
            dest=constants.OUTPUT_NOTEBOOK_ARG,
            required=False,
            default=None,
            help='Path to save executed notebook (Default: None). '
            'If not provided, no notebook is saved'
        )

        parser.add_argument(
            f'--{constants.MYSQL_HOST_ARG}',
            dest=constants.MYSQL_HOST,
            required=True,
            help='MySQL host or IP address'
        )

        parser.add_argument(
            f'--{constants.MYSQL_PORT_ARG}',
            dest=constants.MYSQL_PORT,
            default="3306",
            required=False,
            help='MySQL port (Default: 3306)'
        )

        parser.add_argument(
            f'--{constants.MYSQL_USERNAME_ARG}',
            dest=constants.MYSQL_USERNAME,
            required=True,
            help='MySQL username'
        )

        parser.add_argument(
            f'--{constants.MYSQL_PASSWORD_ARG}',
            dest=constants.MYSQL_PASSWORD,
            required=True,
            help='MySQL password'
        )

        parser.add_argument(
            f'--{constants.MYSQL_DATABASE_ARG}',
            dest=constants.MYSQL_DATABASE,
            required=True,
            help='MySQL database name'
        )

        parser.add_argument(
            f'--{constants.MYSQLTABLE_LIST_ARG}',
            dest=constants.MYSQLTABLE_LIST,
            required=False,
            default='',
            help='MySQL table list to migrate. '
            'Leave empty for migrating complete database else provide tables as \"table1,table2\"'
        )

        parser.add_argument(
            f'--{constants.MYSQL_OUTPUT_SPANNER_MODE_ARG}',
            dest=constants.MYSQL_OUTPUT_SPANNER_MODE,
            required=False,
            default=constants.OUTPUT_MODE_OVERWRITE,
            help='Spanner output write mode (Default: overwrite). '
            'Use append when schema already exists in Spanner',
            choices=[
                constants.OUTPUT_MODE_OVERWRITE,
                constants.OUTPUT_MODE_APPEND
            ]
        )

        parser.add_argument(
            f'--{constants.SPANNER_INSTANCE_ARG}',
            dest=constants.SPANNER_INSTANCE,
            required=True,
            help='Spanner instance name'
        )

        parser.add_argument(
            f'--{constants.SPANNER_DATABASE_ARG}',
            dest=constants.SPANNER_DATABASE,
            required=True,
            help='Spanner database name'
        )

        parser.add_argument(
            f'--{constants.SPANNER_TABLE_PRIMARY_KEYS_ARG}',
            dest=constants.SPANNER_TABLE_PRIMARY_KEYS,
            required=True,
            help='Provide table & PK column which do not have PK in MySQL table {\"table_name\":\"primary_key\"}'
        )

        parser.add_argument(
            f'--{constants.MAX_PARALLELISM_ARG}',
            dest=constants.MAX_PARALLELISM,
            type=int,
            default=5,
            required=False,
            help='Maximum number of tables that will migrated parallelly (Default: 5)'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args()

        return vars(known_args)


    def get_env_var(self, parameters) ->  Dict[str, Any]:
        """
        Get the environment variables.
        """
        parameters[constants.PROJECT] = environ[constants.GCP_PROJECT]
        parameters[constants.REGION] = environ[constants.REGION]
        parameters[constants.GCS_STAGING_LOCATION] = environ[constants.GCS_STAGING_LOCATION]
        parameters[constants.SUBNET] = environ[constants.SUBNET]
        parameters[constants.IS_PARAMETERIZED] = True

        return parameters


    def run(self, args: Dict[str, Any]) -> None:
        """
        Run the notebook.
        """

        # Convert comma separated string to list
        args[constants.MYSQLTABLE_LIST] = list(
            map(str.strip, args[constants.MYSQLTABLE_LIST].split(","))
        )
        # Convert JSON string to object
        args[constants.SPANNER_TABLE_PRIMARY_KEYS] = json.loads(
            args[constants.SPANNER_TABLE_PRIMARY_KEYS]
        )

        # Exclude arguments that are not needed to be passed to the notebook
        ignore_keys = {constants.OUTPUT_NOTEBOOK_ARG}
        nb_parameters = {key:val for key,val in args.items() if key not in ignore_keys}

        # Get environment variables
        nb_parameters = self.get_env_var(nb_parameters)

        # Run the notebook
        output_path = args[constants.OUTPUT_NOTEBOOK_ARG]
        pm.execute_notebook(
            'mysql2spanner/MySqlToSpanner_notebook.ipynb',
            output_path,
            nb_parameters
        )
