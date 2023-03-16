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

import re
from textwrap import dedent
from typing import List, Optional, Tuple, TYPE_CHECKING

from util.jdbc.jdbc_input_manager_interface import (
    JDBCInputManagerInterface,
    JDBCInputManagerException,
    SPARK_PARTITION_COLUMN,
    SPARK_NUM_PARTITIONS,
    SPARK_LOWER_BOUND,
    SPARK_UPPER_BOUND,
    PARTITION_COMMENT,
)

if TYPE_CHECKING:
    import sqlalchemy


class MySQLInputManager(JDBCInputManagerInterface):
    # Private methods

    def _build_table_list(
        self,
        schema_filter: Optional[str] = None,
        table_filter: Optional[List[str]] = None,
    ) -> Tuple[str, List[str]]:
        """
        Return a tuple containing schema and list of table names based on optional table filter.
        schema_filter is unused because it is derived from connected database in MySQL.
        """
        with self._alchemy_db.connect() as conn:
            schema = self._normalise_schema_filter(schema_filter, conn)
            sql = f"show tables;"
            rows = conn.execute(sql).fetchall()
            tables = [_[0] for _ in rows] if rows else rows
            return schema, self._filter_table_list(tables, table_filter)

    def _define_read_partitioning(
        self,
        table: str,
        row_count_threshold: int,
        sa_connection: "sqlalchemy.engine.base.Connection",
    ) -> str:
        """Return a dictionary defining how to partition the Spark SQL extraction."""
        row_count = self._get_table_count_from_stats(table, sa_connection=sa_connection)
        if not row_count:
            # In case this is a new table with no stats, do a full count
            row_count = self._get_table_count(table, sa_connection=sa_connection)

        if row_count < int(row_count_threshold):
            # The table does not have enough rows to merit partitioning Spark SQL read.
            return None

        pk_cols = self.get_primary_keys().get(table)
        if pk_cols and len(pk_cols) == 1:
            column = pk_cols[0]
            column_datatype = self._get_column_data_type(table, column)
            if column_datatype in ("int", "bigint", "mediumint"):
                lowerbound = sa_connection.execute(
                    self._get_min_sql(table, column)
                ).fetchone()
                upperbound = sa_connection.execute(
                    self._get_max_sql(table, column)
                ).fetchone()
                if lowerbound and upperbound:
                    lowerbound = lowerbound[0]
                    upperbound = upperbound[0]
                    num_partitions = self._read_partitioning_num_partitions(
                        lowerbound, upperbound, row_count_threshold
                    )
                    return {
                        SPARK_PARTITION_COLUMN: column,
                        SPARK_NUM_PARTITIONS: num_partitions,
                        SPARK_LOWER_BOUND: lowerbound,
                        SPARK_UPPER_BOUND: upperbound,
                        PARTITION_COMMENT: f"Partitioning by {column_datatype} primary key column",
                    }

    def _enclose_identifier(self, identifier, ch: Optional[str] = None):
        """Enclose an identifier in the standard way for the SQL engine."""
        ch = ch or "`"
        return f"{ch}{identifier}{ch}"

    def _get_column_data_type(
        self,
        table: str,
        column: str,
        sa_connection: "Optional[sqlalchemy.engine.base.Connection]" = None,
    ) -> str:
        # TODO Does MySQL support parameterised queries?
        sql = dedent(
            """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = '{}'
        AND table_name = '{}'
        AND column_name = '{}'
        """.format(
                self._schema, table, column
            )
        )
        if sa_connection:
            row = sa_connection.execute(sql).fetchone()
        else:
            with self._alchemy_db.connect() as conn:
                row = conn.execute(sql).fetchone()
        return row[0] if row else row

    def _get_primary_keys(self) -> dict:
        """
        Return a dict of primary key information.
        The dict is keyed on table name and maps to a list of column names.
        """
        pk_dict = {_: None for _ in self._table_list}
        with self._alchemy_db.connect() as conn:
            for table in self._table_list:
                sql = "SHOW KEYS FROM {} WHERE Key_name = 'PRIMARY'".format(table)
                rows = conn.execute(sql, own=self._schema, tab=table).fetchall()
                if rows:
                    pk_dict[table] = [_[4] for _ in rows]
            return pk_dict

    def _get_table_count_from_stats(
        self,
        table: str,
        sa_connection: "Optional[sqlalchemy.engine.base.Connection]" = None,
    ) -> Optional[int]:
        """Return table count from stats gathering rather than running count(*)."""
        sql = dedent(
            """
            SELECT table_rows
            FROM information_schema.tables
            WHERE table_schema = '{}'
            AND table_name = '{}'
            """.format(
                self._schema, table
            )
        )
        if sa_connection:
            row = sa_connection.execute(sql, own=self._schema, tab=table).fetchone()
        else:
            with self._alchemy_db.connect() as conn:
                row = conn.execute(sql, own=self._schema, tab=table).fetchone()
        return row[0] if row else row

    def _normalise_schema_filter(
        self, schema_filter: str, sa_connection: "sqlalchemy.engine.base.Connection"
    ) -> str:
        """Not used for MySQL."""
        sql = "SELECT DATABASE()"
        row = sa_connection.execute(sql).fetchone()
        if row and schema_filter and schema_filter.upper() != row[0].upper():
            raise JDBCInputManagerException(
                f"Schema filter does not match connected database: {schema_filter} != {row[0]}"
            )
        return row[0] if row else row
