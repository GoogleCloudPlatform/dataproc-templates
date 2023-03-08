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
from typing import List, Optional, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import sqlalchemy


SPARK_PARTITION_COLUMN = 'partitionColumn'
SPARK_NUM_PARTITIONS = 'numPartitions'
SPARK_LOWER_BOUND = 'lowerBound'
SPARK_UPPER_BOUND = 'upperBound'


class JDBCInputManagerInterface(AbstractClass):
    """Defines common code across each engine and enforces methods each engine should provide."""

    def __init__(self, alchemy_db: "sqlalchemy.engine.base.Engine"):
        self._alchemy_db = alchemy_db
        self._schema = None
        self._table_list = []
        self._pk_dict = {}

    # Abstract methods

    @abstractmethod
    def _build_table_list(self, schema_filter: Optional[str] = None) -> Tuple[str, List[str]]:
        """Engine specific code to return a tuple containing schema and list of table names based on an optional schema filter."""

    @abstractmethod
    def _define_read_partitioning(self, table: str, row_count_threshold: int, sa_connection) -> str:
        """Return a dictionary defining how to partition the Spark SQL extraction."""

    @abstractmethod
    def _enclose_identifier(self, identifier, ch: Optional[str] = None):
        """Enclose an identifier in the standard way for the SQL engine or override ch for any enclosure character."""

    @abstractmethod
    def _get_column_data_type(self, schema: str, table: str, column: str, sa_connection=None) -> str:
        """Return base data type for a column without any scale/precision/length annotation."""

    @abstractmethod
    def _get_primary_keys(self) -> dict:
        """
        Return a dict of primary key information.
        The dict is keyed on table name and maps to the column name.
        """

    @abstractmethod
    def _normalise_schema_filter(self, schema_filter: str, sa_connection) -> str:
        """Return schema_filter normalised to the correct case."""

    @abstractmethod
    def _qualified_name(self, schema: str, table: str, enclosed=False) -> str:
        """Return a qualified name for a table suitable for the SQL engine."""

    # Private methods

    def _get_count_sql(self, table: str) -> str:
        # This SQL should be simple enough to work on all engines but may need refactoring in the future.
        return "SELECT COUNT(*) FROM {}".format(
            self._qualified_name(self._schema, table, enclosed=True)
        )

    def _get_max_sql(self, table: str, column: str) -> str:
        # This SQL should be simple enough to work on all engines but may need refactoring in the future.
        return "SELECT MAX({0}) FROM {1} WHERE {0} IS NOT NULL".format(
            self._enclose_identifier(column),
            self._qualified_name(self._schema, table, enclosed=True)
        )

    def _get_min_sql(self, table: str, column: str) -> str:
        # This SQL should be simple enough to work on all engines but may need refactoring in the future.
        return "SELECT MIN({0}) FROM {1} WHERE {0} IS NOT NULL".format(
            self._enclose_identifier(column),
            self._qualified_name(self._schema, table, enclosed=True)
        )

    def _get_table_count(self, table: str, sa_connection=None) -> Optional[int]:
        """Return row count for a table."""
        sql = self._get_count_sql(self._schema, table)
        if sa_connection:
            row = sa_connection.execute(sql).fetchone()
        else:
            with self._alchemy_db.connect() as conn:
                row = conn.execute(sql).fetchone()
        return row[0] if row else row

    def _read_partitioning_num_partitions(self, lowerbound, upperbound, stride):
        """Return appropriate Spark SQL numPartition value for input range/stride."""
        assert stride > 0
        if isinstance(lowerbound, (int, float, Decimal)) and isinstance(upperbound, (int, float, Decimal)):
            return math.ceil(float(upperbound - lowerbound) / float(stride))
        else:
            raise NotImplementedError(f'Unsupported partition boundary values: {type(lowerbound)}/{type(upperbound)}')

    # Public methods

    def build_table_list(self, schema_filter: Optional[list] = None) -> List[tuple]:
        """
        Return a list of (schema, table_name) tuples based on an optional schema filter.
        If schema_filter is not provided then the connected user is used for the schema.
        """
        self._schema, self._table_list = self._build_table_list(schema_filter)
        return self._table_list

    def define_read_partitioning(self, row_count_threshold: int) -> dict:
        """Return a dictionary defining how to partition the Spark SQL extraction."""
        read_partition_info = {}
        with self._alchemy_db.connect() as conn:
            for table in self._table_list:
                partition_options = self._define_read_partitioning(table, row_count_threshold, conn)
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
        The dict is keyed on the qualified table name (e.g. 'schema.table_name') and maps to the column name.
        """
        if not self._pk_dict:
            self._pk_dict = self._get_primary_keys()
        return self._pk_dict

    def normalise_schema_filter(self, schema_filter: Optional[str]) -> str:
        """Return schema_filter normalised to the correct case."""
        return self._normalise_schema_filter(schema_filter)

    def qualified_name(self, schema: str, table: str, enclosed=False) -> str:
        return self._qualified_name(schema, table, enclosed=enclosed)

    def set_table_list(self, table_list: list) -> None:
        self._table_list = table_list
