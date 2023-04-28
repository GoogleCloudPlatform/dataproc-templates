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
import argparse
import json

import papermill as pm
import util.template_constants as constants

def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        f'--{constants.MYSQL_HOST}',
        dest=constants.MYSQL_HOST,
        required=True,
        help=''
    )

    parser.add_argument(
        f'--{constants.MYSQL_PORT}',
        dest=constants.MYSQL_PORT,
        required=True,
        help=''
    )

    parser.add_argument(
        f'--{constants.MYSQL_USERNAME}',
        dest=constants.MYSQL_USERNAME,
        required=True,
        help=''
    )

    parser.add_argument(
        f'--{constants.MYSQL_PASSWORD}',
        dest=constants.MYSQL_PASSWORD,
        required=True,
        help=''
    )

    parser.add_argument(
        f'--{constants.MYSQL_DATABASE}',
        dest=constants.MYSQL_DATABASE,
        required=True,
        help=''
    )

    parser.add_argument(
        f'--{constants.MYSQLTABLE_LIST}',
        dest=constants.MYSQLTABLE_LIST,
        required=True,
        help=''
    )

    parser.add_argument(
        f'--{constants.MYSQL_OUTPUT_SPANNER_MODE}',
        dest=constants.MYSQL_OUTPUT_SPANNER_MODE,
        required=True,
        help=''
    )

    parser.add_argument(
        f'--{constants.SPANNER_INSTANCE}',
        dest=constants.SPANNER_INSTANCE,
        required=True,
        help=''
    )

    parser.add_argument(
        f'--{constants.SPANNER_DATABASE}',
        dest=constants.SPANNER_DATABASE,
        required=True,
        help=''
    )

    parser.add_argument(
        f'--{constants.SPANNER_TABLE_PRIMARY_KEYS}',
        dest=constants.SPANNER_TABLE_PRIMARY_KEYS,
        required=True,
        help=''
    )

    parser.add_argument(
        f'--{constants.MAX_PARALLELISM}',
        dest=constants.MAX_PARALLELISM,
        type=int,
        required=True,
        help=''
    )

    known_args: argparse.Namespace
    known_args, _ = parser.parse_known_args()

    return vars(known_args)


def get_env_var(known_args) -> argparse.Namespace:
    """
    Get the environment variable.
    """
    known_args[constants.PROJECT] = environ[constants.GCP_PROJECT]
    known_args[constants.REGION] = environ[constants.REGION]
    known_args[constants.GCS_STAGING_LOCATION] = "gs://" + \
        environ[constants.GCS_STAGING_LOCATION]
    known_args[constants.SUBNET] = environ[constants.SUBNET]


def run(known_args) -> None:
    """
    Run the notebook.
    """

    pm.execute_notebook(
        'mysql2spanner/MySqlToSpanner_notebook.ipynb',
        'mysql2spanner/OUTPUT_MySqlToSpanner_notebook.ipynb',
        parameters=known_args
    )


def main():
    """
    Main entry point into the script.
    """
    # Get command line arguments
    known_args = parse_args()
    known_args[constants.MYSQLTABLE_LIST] = list(
        map(str.strip, known_args[constants.MYSQLTABLE_LIST].split(","))
    )
    known_args[constants.SPANNER_TABLE_PRIMARY_KEYS] = json.loads(
        known_args[constants.SPANNER_TABLE_PRIMARY_KEYS]
    )

    # Get environment variables
    get_env_var(known_args)

    # Run the notebook
    run(known_args)


if __name__ == "__main__":
    main()
