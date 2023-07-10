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

from typing import Dict, Any, Type

from parameterize_script import BaseParameterizeScript, ScriptName
import parameterize_script.util.notebook_constants as constants
from parameterize_script.util import get_script_name
from hive2bq import HiveToBigQueryScript
from mysql2spanner import MySqlToSpannerScript
from oracle2bq import OracleToBigQueryScript
from postgresql2bq import PostgreSqlToBigQueryScript
import logging
from oracle2postgres import OracleToPostgresScript

# Maps each ScriptName to its corresponding implementation
# of BaseParameterizeScript
SCRIPT_IMPLS: Dict[ScriptName, Type[BaseParameterizeScript]] = {
    ScriptName.HIVETOBIGQUERY: HiveToBigQueryScript,
    ScriptName.MYSQLTOSPANNER: MySqlToSpannerScript,
    ScriptName.ORACLETOBIGQUERY: OracleToBigQueryScript,
    ScriptName.ORACLETOPOSTGRES: OracleToPostgresScript,
    ScriptName.POSTGRESTOBIGQUERY:PostgreSqlToBigQueryScript

}

def run_script(script_name: ScriptName) -> None:
    """
    Executes a script given it's script name.
    Args:
        script_name (ScriptName): The ScriptName of the script
            that should be run.
    Returns:
        None
    """

    script_impl: Type[BaseParameterizeScript] = SCRIPT_IMPLS[script_name]
    script_instance: BaseParameterizeScript = script_impl.build()
    args: Dict[str, Any] = script_instance.parse_args()
    logging.basicConfig(level=args[constants.LOG_LEVEL_ARG], format="%(message)s")
    script_instance.run(args=args)

if __name__ == '__main__':

    run_script(
        script_name=get_script_name()
    )
