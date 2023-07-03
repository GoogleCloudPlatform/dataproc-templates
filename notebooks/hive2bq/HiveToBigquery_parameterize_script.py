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

import papermill as pm

from parameterize_script import BaseParameterizeScript
import parameterize_script.util.notebook_constants as constants
from parameterize_script.util import get_common_args

__all__ = ['HiveToBigQueryScript']


class HiveToBigQueryScript(BaseParameterizeScript):

    """
    Script to parameterize HiveToBigQuery notebook.
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.HIVE_METASTORE_ARG}',
            dest=constants.HIVE_METASTORE,
            required=True,
            help='Hive metastore URI'
        )

        parser.add_argument(
            f'--{constants.INPUT_HIVE_DATABASE_ARG}',
            dest=constants.INPUT_HIVE_DATABASE,
            required=True,
            help='Hive database name'
        )

        parser.add_argument(
            f'--{constants.INPUT_HIVE_TABLES_ARG}',
            dest=constants.INPUT_HIVE_TABLES,
            required=False,
            default="*",
            help='Comma separated list of Hive tables to be migrated "/table1,table2,.../" (Default: *)'
        )

        parser.add_argument(
            f'--{constants.OUTPUT_BIGQUERY_DATASET_ARG}',
            dest=constants.OUTPUT_BIGQUERY_DATASET,
            required=True,
            help='BigQuery dataset name'
        )

        parser.add_argument(
            f'--{constants.TEMP_BUCKET_ARG}',
            dest=constants.TEMP_BUCKET,
            required=True,
            help='Cloud Storage bucket name for temporary staging'
        )

        parser.add_argument(
            f'--{constants.HIVE_OUTPUT_MODE_ARG}',
            dest=constants.HIVE_OUTPUT_MODE,
            required=False,
            default=constants.OUTPUT_MODE_OVERWRITE,
            help='Hive output mode (Default: overwrite)',
            choices=[constants.OUTPUT_MODE_OVERWRITE,
                     constants.OUTPUT_MODE_APPEND]
        )

        parser.add_argument(
            f'--{constants.MAX_PARALLELISM_ARG}',
            dest=constants.MAX_PARALLELISM,
            type=int,
            default=5,
            required=False,
            help='Maximum number of tables that will migrated parallelly (Default: 5)'
        )

        parser.add_argument(
            f'--{constants.BQ_DATASET_REGION_ARG}',
            dest=constants.BQ_DATASET_REGION,
            required=False,
            default="us",
            help='BigQuery dataset region (Default: us)'
        )

        parser = get_common_args(parser)

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args()

        return vars(known_args)

    def run(self, args: Dict[str, Any]) -> None:
        """
        Run the notebook.
        """

        # Exclude arguments that are not needed to be passed to the notebook
        ignore_keys = {constants.LOG_LEVEL_ARG, constants.OUTPUT_NOTEBOOK_ARG}
        nb_parameters = {key:val for key,val in args.items() if key not in ignore_keys}

        # Get environment variables
        env_vars = HiveToBigQueryScript.get_env_vars()
        nb_parameters.update(env_vars)

        # Run the notebook
        output_path = args[constants.OUTPUT_NOTEBOOK_ARG]
        pm.execute_notebook(
            'hive2bq/HiveToBigquery_notebook.ipynb',
            output_path,
            parameters=nb_parameters,
            log_output=True
        )
