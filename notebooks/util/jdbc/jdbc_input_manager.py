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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlalchemy

from util.jdbc.engines.oracle_input_manager import OracleInputManager
from util.jdbc.engines.mysql_input_manager import MySQLInputManager


DB_TYPE_ORACLE = "oracle"
DB_TYPE_MSSQL = "mssql"
DB_TYPE_MYSQL = "mysql"


class JDBCInputManager:
    @classmethod
    def create(cls, db_type: str, alchemy_db: "sqlalchemy.engine.base.Engine"):
        assert db_type
        if db_type == DB_TYPE_ORACLE:
            return OracleInputManager(alchemy_db)
        elif db_type == DB_TYPE_MYSQL:
            return MySQLInputManager(alchemy_db)
        else:
            raise NotImplementedError(f"Unsupported SQL engine type: {db_type}")
