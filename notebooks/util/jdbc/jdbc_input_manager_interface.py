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

from abc import ABC as AbstractClass, abstractmethod
from decimal import Decimal
import math
from typing import List, Optional, Tuple

import pandas as pd
import sqlalchemy


SPARK_PARTITION_COLUMN = "partitionColumn"
SPARK_NUM_PARTITIONS = "numPartitions"
SPARK_LOWER_BOUND = "lowerBound"
SPARK_UPPER_BOUND = "upperBound"
PARTITION_COMMENT = "comment"


class JDBCInputManagerException(Exception):
    pass


class JDBCInputManagerInterface(AbstractClass):
    """Defines common code across each engine and enforces methods each engine should provide."""

    def __init__(self, alchemy_db: "sqlalchemy.engine.base.Engine"):
        self._alchemy_db = alchemy_db
        self._schema = None
        self._table_list = []
        self._pk_dict = {}

    # Abstract methods

    @abstractmethod
    def _build_table_list(
        self,
        schema_filter: Optional[str] = None,
        table_filter: Optional[List[str]] = None,
    ) -> Tuple[str, List[str]]:
        """Engine specific code to return a tuple containing schema and list of table names based on optional schema/table filters."""

    @abstractmethod
    def _define_read_partitioning(
        self,
        table: str,
        row_count_threshold: int,
        sa_connection: "sqlalchemy.engine.base.Connection",
        custom_partition_column: Optional[str],
    ) -> str:
        """Return a dictionary defining how to partition the Spark SQL extraction."""

    @abstractmethod
    def _enclose_identifier(self, identifier, ch: Optional[str] = None):
        """Enclose an identifier in the standard way for the SQL engine or override ch for any enclosure character."""

    @abstractmethod
    def _get_column_data_type(
        self,
        table: str,
        column: str,
        sa_connection: "Optional[sqlalchemy.engine.base.Connection]" = None,
    ) -> str:
        """Return base data type for a column without any scale/precision/length annotation."""

    @abstractmethod
    def _get_primary_keys(self) -> dict:
        """
        Return a dict of primary key information.
        The dict is keyed on the qualified table name (e.g. 'schema.table_name') and
        maps to a list of primary key column names.
        """

    @abstractmethod
    def _get_table_count_from_stats(
        self,
        table: str,
        sa_connection: "Optional[sqlalchemy.engine.base.Connection]" = None,
    ) -> Optional[int]:
        """Return table count from stats gathering rather than running count(*)."""

    @abstractmethod
    def _normalise_schema_filter(
        self, schema_filter: str, sa_connection: "sqlalchemy.engine.base.Connection"
    ) -> str:
        """Return schema_filter normalised to the correct case."""

    # Private methods

    def _define_native_column_read_partitioning(
        self,
        table: str,
        column: str,
        accepted_data_types: List[str],
        row_count: int,
        row_count_threshold: int,
        column_description: str,
        sa_connection: "sqlalchemy.engine.base.Connection",
    ):
        column_datatype = self._get_column_data_type(table, column)
        if column_datatype in accepted_data_types:
            lowerbound = self._get_table_min(table, column, sa_connection=sa_connection)
            upperbound = self._get_table_max(table, column, sa_connection=sa_connection)
            if lowerbound and upperbound:
                # TODO Really we should define num_partitions as ceil(table row count / threshold)
                #      and not as in _read_partitioning_num_partitions() but leaving logic
                #      as-is for now, we can revisit in the future.
                num_partitions = self._read_partitioning_num_partitions(
                    row_count, row_count_threshold
                )
                return {
                    SPARK_PARTITION_COLUMN: column,
                    SPARK_NUM_PARTITIONS: num_partitions,
                    SPARK_LOWER_BOUND: lowerbound,
                    SPARK_UPPER_BOUND: upperbound,
                    PARTITION_COMMENT: f"Partitioning by {column_datatype} {column_description}",
                }
        return None

    def _filter_table_list(self, table_list: List[str], table_filter: List[str]):
        """Returns table_list filtered for entries (case-insensitive) in table_filter."""

        def table_name(s):
            """Cater for passing of row returned from SQL which will have the table_name in a list/tuple."""
            return s[0] if isinstance(s, (list, tuple)) else s

        if table_filter:
            table_filter_upper = [_.upper() for _ in table_filter or []]
            return [
                table_name(_)
                for _ in table_list
                if table_name(_).upper() in table_filter_upper
            ]
        else:
            return [table_name(_) for _ in table_list]

    def _get_count_sql(self, table: str) -> str:
        # This SQL should be simple enough to work on all engines but may need refactoring in the future.
        return "SELECT COUNT(*) FROM {}".format(
            self.qualified_name(self._schema, table, enclosed=True)
        )

    def _get_max_sql(self, table: str, column: str) -> str:
        # This SQL should be simple enough to work on all engines but may need refactoring in the future.
        return "SELECT MAX({0}) FROM {1} WHERE {0} IS NOT NULL".format(
            self._enclose_identifier(column),
            self.qualified_name(self._schema, table, enclosed=True),
        )

    def _get_min_sql(self, table: str, column: str) -> str:
        # This SQL should be simple enough to work on all engines but may need refactoring in the future.
        return "SELECT MIN({0}) FROM {1} WHERE {0} IS NOT NULL".format(
            self._enclose_identifier(column),
            self.qualified_name(self._schema, table, enclosed=True),
        )

    def _get_table_count(
        self,
        table: str,
        sa_connection: "Optional[sqlalchemy.engine.base.Connection]" = None,
    ) -> Optional[int]:
        """Return row count for a table."""
        sql = self._get_count_sql(table)
        if sa_connection:
            row = sa_connection.execute(sqlalchemy.text(sql)).fetchone()
        else:
            with self._alchemy_db.connect() as conn:
                row = conn.execute(sqlalchemy.text(sql)).fetchone()
        return row[0] if row else row

    def _get_table_min(
        self,
        table: str,
        column: str,
        sa_connection: "Optional[sqlalchemy.engine.base.Connection]" = None,
    ) -> Optional[int]:
        """Return min(column) for a table."""
        sql = self._get_min_sql(table, column)
        if sa_connection:
            row = sa_connection.execute(sqlalchemy.text(sql)).fetchone()
        else:
            with self._alchemy_db.connect() as conn:
                row = conn.execute(sqlalchemy.text(sql)).fetchone()
        return row[0] if row else row

    def _get_table_max(
        self,
        table: str,
        column: str,
        sa_connection: "Optional[sqlalchemy.engine.base.Connection]" = None,
    ) -> Optional[int]:
        """Return max(column) for a table."""
        sql = self._get_max_sql(table, column)
        if sa_connection:
            row = sa_connection.execute(sqlalchemy.text(sql)).fetchone()
        else:
            with self._alchemy_db.connect() as conn:
                row = conn.execute(sqlalchemy.text(sql)).fetchone()
        return row[0] if row else row

    def _read_partitioning_num_partitions(self, row_count: int, stride: int) -> int:
        """Return appropriate Spark SQL numPartition value for input range/row count."""
        assert row_count >= 0
        assert stride >= 0
        if not row_count or not stride:
            return 1
        return math.ceil(row_count / stride)

    # Public methods

    def build_table_list(
        self,
        schema_filter: Optional[list] = None,
        table_filter: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Return a list of (schema, table_name) tuples based on an optional schema filter.
        If schema_filter is not provided then the connected user is used for the schema.
        """
        self._schema, self._table_list = self._build_table_list(
            schema_filter=schema_filter, table_filter=table_filter
        )
        return self._table_list

    def define_read_partitioning(
        self,
        row_count_threshold: int,
        custom_partition_columns: Optional[dict] = None,
    ) -> dict:
        """
        Return a dictionary defining how to partition the Spark SQL extraction.
        custom_partition_columns is an optional dict allowing the user to provide
        any column name as a read partition column.
        """
        read_partition_info = {}
        # Case insensitive match for custom_partition_column in case user was imprecise.
        custom_partition_columns = {
            k.upper(): v for k, v in custom_partition_columns.items()
        }
        with self._alchemy_db.connect() as conn:
            for table in self._table_list:
                partition_options = self._define_read_partitioning(
                    table,
                    row_count_threshold,
                    conn,
                    custom_partition_column=(custom_partition_columns or {}).get(
                        table.upper()
                    ),
                )
                if partition_options:
                    read_partition_info[table] = partition_options
            return read_partition_info

    def get_schema(self) -> str:
        return self._schema

    def get_table_list(self) -> List[tuple]:
        return self._table_list

    def get_table_list_with_counts(self) -> List[int]:
        """Return a list of table counts in the same order as the list of tables."""
        counts = []
        with self._alchemy_db.connect() as conn:
            for table in self.get_table_list():
                counts.append(self._get_table_count(table, sa_connection=conn))
            return counts

    def get_primary_keys(self) -> dict:
        """
        Return a dict of primary key information.
        The dict is keyed on the qualified table name (e.g. 'schema.table_name') and
        maps to a list of primary key column names.
        """
        if not self._pk_dict:
            self._pk_dict = self._get_primary_keys()
        return self._pk_dict

    def normalise_schema(self, schema_filter: str) -> str:
        with self._alchemy_db.connect() as conn:
            self._schema = self._normalise_schema_filter(schema_filter, conn)
            return self._schema

    def qualified_name(self, schema: str, table: str, enclosed=False) -> str:
        if enclosed:
            return (
                self._enclose_identifier(schema) + "." + self._enclose_identifier(table)
            )
        else:
            return schema + "." + table

    def read_partitioning_df(self, read_partition_info: dict) -> pd.DataFrame:
        """Return a Pandas dataframe to allow tidy display of read partitioning information"""

        def get_read_partition_info(table, info_key):
            return read_partition_info.get(table, {}).get(info_key)

        report_dict = {
            "table": self.get_table_list(),
            "partition_column": [
                get_read_partition_info(_, SPARK_PARTITION_COLUMN)
                for _ in self.get_table_list()
            ],
            "num_partitions": [
                get_read_partition_info(_, SPARK_NUM_PARTITIONS)
                for _ in self.get_table_list()
            ],
            "lower_bound": [
                get_read_partition_info(_, SPARK_LOWER_BOUND)
                for _ in self.get_table_list()
            ],
            "upper_bound": [
                get_read_partition_info(_, SPARK_UPPER_BOUND)
                for _ in self.get_table_list()
            ],
            "comment": [
                get_read_partition_info(_, PARTITION_COMMENT) or "Serial read"
                for _ in self.get_table_list()
            ],
        }
        return pd.DataFrame(report_dict)

    def set_table_list(self, table_list: list) -> None:
        self._table_list = table_list
