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

from parameterize_script.util import get_common_args
from typing import Dict, Sequence, Optional, Any
import argparse
import json

import papermill as pm

from parameterize_script import BaseParameterizeScript
import parameterize_script.util.notebook_constants as constants

__all__ = ["OracleToPostgresScript"]


class OracleToPostgresScript(BaseParameterizeScript):

    """
    Script to parameterize OracleToPostgres notebook.
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            f"--{constants.ORACLE_HOST_ARG}",
            dest=constants.ORACLE_HOST,
            required=True,
            help="Oracle host or IP address",
        )

        parser.add_argument(
            f"--{constants.ORACLE_PORT_ARG}",
            dest=constants.ORACLE_PORT,
            default="1521",
            required=False,
            help="Oracle port (Default: 1521)",
        )

        parser.add_argument(
            f"--{constants.ORACLE_USERNAME_ARG}",
            dest=constants.ORACLE_USERNAME,
            required=True,
            help="Oracle username",
        )

        parser.add_argument(
            f"--{constants.ORACLE_PASSWORD_ARG}",
            dest=constants.ORACLE_PASSWORD,
            required=True,
            help="Oracle password",
        )

        parser.add_argument(
            f"--{constants.ORACLE_DATABASE_ARG}",
            dest=constants.ORACLE_DATABASE,
            required=True,
            help="Oracle database name",
        )

        parser.add_argument(
            f"--{constants.ORACLE_TABLE_LIST_ARG}",
            dest=constants.ORACLE_TABLE_LIST,
            required=False,
            help="Oracle table list to migrate. "
            'Leave empty for migrating complete database else provide tables as "table1,table2"',
        )

        parser.add_argument(
            f"--{constants.POSTGRES_HOST_ARG}",
            dest=constants.POSTGRES_HOST,
            required=True,
            help="Postgres host or IP address",
        )
        parser.add_argument(
            f"--{constants.POSTGRES_PORT_ARG}",
            dest=constants.POSTGRES_PORT,
            required=True,
            help="Postgres post",
        )
        parser.add_argument(
            f"--{constants.POSTGRES_USERNAME_ARG}",
            dest=constants.POSTGRES_USERNAME,
            required=True,
            help="Postgres Username",
        )
        parser.add_argument(
            f"--{constants.POSTGRES_PASSWORD_ARG}",
            dest=constants.POSTGRES_PASSWORD,
            required=True,
            help="Postgres Password",
        )
        parser.add_argument(
            f"--{constants.POSTGRES_DATABASE_ARG}",
            dest=constants.POSTGRES_DATABASE,
            required=True,
            help="Postgres database name",
        )
        parser.add_argument(
            f"--{constants.POSTGRES_SCHEMA_ARG}",
            dest=constants.POSTGRES_SCHEMA,
            required=True,
            help="Postgres Schema name where we want to migrate",
        )
        parser.add_argument(
            f"--{constants.MAX_PARALLELISM_ARG}",
            dest=constants.MAX_PARALLELISM,
            type=int,
            default=5,
            required=False,
            help="Maximum number of tables that will migrated parallelly (Default: 5)",
        )
        parser.add_argument(
            f"--{constants.JDBCTOJDBC_OUTPUT_MODE_ARG}",
            dest=constants.JDBCTOJDBC_OUTPUT_MODE,
            required=False,
            default=constants.OUTPUT_MODE_OVERWRITE,
            help="Postgres output write mode (Default: overwrite).",
            choices=[constants.OUTPUT_MODE_OVERWRITE, constants.OUTPUT_MODE_APPEND],
        )
        parser.add_argument(
            f"--{constants.BATCH_SIZE_ARG}",
            dest=constants.BATCH_SIZE,
            required=False,
            default=1000,
            help="JDBC output batch size (Default: 1000).",
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
        if args[constants.ORACLE_TABLE_LIST]:
            args[constants.ORACLE_TABLE_LIST] = list(
                map(str.strip, args[constants.ORACLE_TABLE_LIST].split(","))
            )
        else:
            args[constants.ORACLE_TABLE_LIST] = []

        # Exclude arguments that are not needed to be passed to the notebook
        ignore_keys = {constants.LOG_LEVEL_ARG, constants.OUTPUT_NOTEBOOK_ARG}
        nb_parameters = {
            key: val for key, val in args.items() if key not in ignore_keys
        }

        # Get environment variables
        env_vars = OracleToPostgresScript.get_env_vars()
        nb_parameters.update(env_vars)

        # Run the notebook
        output_path = args[constants.OUTPUT_NOTEBOOK_ARG]
        pm.execute_notebook(
            "oracle2postgres/OracleToPostgres_notebook.ipynb",
            output_path,
            nb_parameters,
            log_output=True,
        )
