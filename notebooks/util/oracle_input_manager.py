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

from textwrap import dedent
from typing import List, Optional, Union

from util.jdbc_input_manager_interface import (
    JDBCInputManagerInterface,
    SPARK_PARTITION_COLUMN,
    SPARK_NUM_PARTITIONS,
    SPARK_LOWER_BOUND,
    SPARK_UPPER_BOUND,
)


class OracleInputManager(JDBCInputManagerInterface):

    # Private methods

    def _build_table_list(self, schema_filter: Optional[Union[list, str]] = None) -> List[tuple]:
        """
        Return a list of (schema, table_name) tuples based on an optional schema filter.
        If schema_filter is not provided then the corrected user is used for the schema.
        """
        if schema_filter:
            if isinstance(schema_filter, str):
                schema_filter = [schema_filter]

        with self._alchemy_db.connect() as conn:
            not_like_filter = "table_name NOT LIKE 'DR$SUP_TEXT_IDX%'"
            schema_filter_dict = {}
            if schema_filter:
                schema_filter_dict = {f's{i}': _ for i, _ in enumerate(schema_filter)}
                schema_filter_str = ','.join([f':{_}' for _ in schema_filter_dict.keys()])
                sql = f'SELECT owner, table_name FROM all_tables WHERE owner IN ({schema_filter_str}) AND {not_like_filter}'
            else:
                sql = f'SELECT USER, table_name FROM user_tables WHERE {not_like_filter}'
            return conn.execute(sql, **schema_filter_dict).fetchall()

    def _define_read_partitioning(self, schema: str, table: str, row_count_threshold: int, sa_connection) -> Optional[list]:
        """Return a dictionary defining how to partition the Spark SQL extraction."""
        # TODO In the future we may want to support checking DBA_SEGMENTS
        row_count = self._get_table_count(schema, table, sa_connection=sa_connection)
        if row_count > int(row_count_threshold):
            # The table has enough rows to merit partitioning Spark SQL read.
            qualified_name = self._qualified_name(schema, table, enclosed=False)
            # TODO Prioritise partition keys over primary keys in the future.
            # TODO Add support for UKs alongside PKs.
            if self.get_primary_keys().get(qualified_name):
                column = self.get_primary_keys().get(qualified_name)
                column_datatype = self._get_column_data_type(schema, table, column)
                if column_datatype == 'NUMBER':
                    lowerbound = sa_connection.execute(self._get_min_sql(schema, table, column)).fetchone()
                    upperbound = sa_connection.execute(self._get_max_sql(schema, table, column)).fetchone()
                    if lowerbound and upperbound:
                        lowerbound = lowerbound[0]
                        upperbound = upperbound[0]
                        num_partitions = self._read_partitioning_num_partitions(lowerbound, upperbound, row_count_threshold)
                        return {
                             SPARK_PARTITION_COLUMN: column,
                             SPARK_NUM_PARTITIONS: num_partitions,
                             SPARK_LOWER_BOUND: lowerbound,
                             SPARK_UPPER_BOUND: upperbound,
                        }
        return None

    def _enclose_identifier(self, identifier, ch: Optional[str] = None):
        """Enclose an identifier in the standard way for the SQL engine."""
        ch = ch or '"'
        return f'{ch}{identifier}{ch}'

    def _get_column_data_type(self, schema: str, table: str, column: str, sa_connection=None) -> str:
        sql = dedent("""
        SELECT data_type
        FROM   all_tab_columns
        WHERE  owner = :own
        AND    table_name = :tab
        AND    column_name = :col
        """)
        if sa_connection:
            row = sa_connection.execute(sql, own=schema, tab=table, col=column).fetchone()
        else:
            with self._alchemy_db.connect() as conn:
                row = conn.execute(sql, own=schema, tab=table, col=column).fetchone()
        if row:
            # TODO we need to strip out any scale from TIMESTAMP types.
            return row[0]
        else:
            return row

    def _get_primary_keys(self) -> dict:
        """
        Return a dict of primary key information.
        The dict is keyed on the qualified table name (e.g. 'schema.table_name') and maps to the column name.
        """
        pk_dict = {_: None for _ in self.get_qualified_table_list()}
        sql = dedent("""
        SELECT cols.column_name
        FROM   all_constraints cons
        ,      all_cons_columns cols
        WHERE  cons.owner = :own
        AND    cons.table_name = :tab
        AND    cons.constraint_type = 'P'
        AND    cons.status = 'ENABLED'
        AND    cols.position = 1
        AND    cols.constraint_name = cons.constraint_name
        AND    cols.owner = cons.owner
        AND    cols.table_name = cons.table_name
        """)
        with self._alchemy_db.connect() as conn:
            for schema, table in self._table_list:
                row = conn.execute(sql, own=schema, tab=table).fetchone()
                if row:
                    pk_dict[self._qualified_name(schema, table)] = row[0]
            return pk_dict

    def _normalise_schema_filter(self, schema_filter: List[str]) -> List[str]:
        """Return schema_filter normalised to the correct case."""
        if not schema_filter:
            return schema_filter
        bind_values = {f'b{i}': _.lower() for i, _ in enumerate(schema_filter)}
        bind_tokens = ','.join([f':{_}' for _ in bind_values.keys()])
        sql = dedent(f"""
        SELECT username
        FROM   all_users
        WHERE  LOWER(user_name) IN ({bind_tokens})
        """)
        with self._alchemy_db.connect() as conn:
            rows = conn.execute(sql, **bind_values).fetchall()
            return [_[0] for _ in rows]

    def _qualified_name(self, schema: str, table: str, enclosed=False) -> str:
        if enclosed:
            return self._enclose_identifier(schema, '"') + "." + self._enclose_identifier(table, '"')
        else:
            return schema + "." + table

    # Public methods
