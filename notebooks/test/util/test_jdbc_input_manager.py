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

from datetime import datetime
from decimal import Decimal
import pytest
from unittest import mock

import pandas as pd

from util.jdbc.jdbc_input_manager import JDBCInputManager
from util.jdbc.jdbc_input_manager_interface import (
    JDBCInputManagerException,
    JDBCInputManagerInterface,
    SPARK_PARTITION_COLUMN,
    SPARK_NUM_PARTITIONS,
    SPARK_LOWER_BOUND,
    SPARK_UPPER_BOUND,
    PARTITION_COMMENT,
)


ALCHEMY_DB = mock.MagicMock()


def test_input_manager_init():
    for db_type in ["oracle", "mysql"]:
        mgr = JDBCInputManager.create(db_type, ALCHEMY_DB)
        assert isinstance(mgr, JDBCInputManagerInterface)


def test_enclose_identifier_oracle():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    assert mgr._enclose_identifier("a", "'") == "'a'"
    assert mgr._enclose_identifier("a") == '"a"'
    assert mgr._enclose_identifier("a", '"') == '"a"'
    assert mgr._enclose_identifier("A", '"') == '"A"'


def test_enclose_identifier_mysql():
    mgr = JDBCInputManager.create("mysql", ALCHEMY_DB)
    assert mgr._enclose_identifier("a", "'") == "'a'"
    assert mgr._enclose_identifier("a") == "`a`"
    assert mgr._enclose_identifier("a", "`") == "`a`"
    assert mgr._enclose_identifier("A", "`") == "`A`"


def test_filter_table_list():
    # This is the same for all engines therefore no need to test across all.
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    table_list = ["table1", "TABLE2", "Table3"]
    assert mgr._filter_table_list(table_list, ["TABLE1", "table2", "table4"]) == [
        "table1",
        "TABLE2",
    ]
    assert mgr._filter_table_list(table_list, None) == table_list
    assert mgr._filter_table_list(table_list, []) == table_list
    # table_list can be list of tuples if fed directly from SQL output.
    assert mgr._filter_table_list([(_,) for _ in table_list], ["TABLE1"]) == ["table1"]
    assert mgr._filter_table_list([(_,) for _ in table_list], []) == table_list


def test_qualified_name_oracle():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    assert mgr.qualified_name("SCHEMA1", "TABLE1", enclosed=False) == "SCHEMA1.TABLE1"
    assert (
        mgr.qualified_name("SCHEMA1", "TABLE1", enclosed=True) == '"SCHEMA1"."TABLE1"'
    )


def test_qualified_name_mysql():
    mgr = JDBCInputManager.create("mysql", ALCHEMY_DB)
    assert mgr.qualified_name("SCHEMA1", "TABLE1", enclosed=False) == "SCHEMA1.TABLE1"
    assert (
        mgr.qualified_name("SCHEMA1", "TABLE1", enclosed=True) == "`SCHEMA1`.`TABLE1`"
    )


def test_table_list():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    table_list = ["table1", "TABLE2"]
    mgr.set_table_list(table_list)
    assert mgr.get_table_list() == table_list


def test_get_table_list_with_counts():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    mgr._get_table_count = mock.MagicMock(return_value=42)
    table_list = ["table1", "table2"]
    mgr.set_table_list(table_list)
    assert mgr.get_table_list_with_counts() == [42, 42]


def test_define_native_column_read_partitioning_oracle():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    mgr._get_table_min = mock.MagicMock(return_value=1)
    mgr._get_table_max = mock.MagicMock(return_value=100)
    mgr._get_column_data_type = mock.MagicMock(return_value="NUMBER")
    part_opts = mgr._define_native_column_read_partitioning(
        "table1",
        "column",
        ["NUMBERxxx"],
        99,
        50,
        "test column",
        None,
    )
    assert part_opts is None

    # Accepted numeric partition columns
    for data_type in ["NUMBER"]:
        mgr._get_column_data_type = mock.MagicMock(return_value=data_type)
        part_opts = mgr._define_native_column_read_partitioning(
            "table1",
            "column",
            ["NUMBER"],
            99,
            50,
            "test column",
            None,
        )
        assert part_opts == {
            SPARK_PARTITION_COLUMN: "column",
            SPARK_NUM_PARTITIONS: 2,
            SPARK_LOWER_BOUND: 1,
            SPARK_UPPER_BOUND: 100,
            PARTITION_COMMENT: f"Partitioning by {data_type} test column",
        }


def test_define_native_column_read_partitioning_mysql():
    mgr = JDBCInputManager.create("mysql", ALCHEMY_DB)
    mgr._get_table_min = mock.MagicMock(return_value=1)
    mgr._get_table_max = mock.MagicMock(return_value=100)
    mgr._get_column_data_type = mock.MagicMock(return_value="int")
    part_opts = mgr._define_native_column_read_partitioning(
        "table1",
        "column",
        ["intxxx"],
        99,
        50,
        "test column",
        None,
    )
    assert part_opts is None

    # Accepted numeric partition columns
    for data_type in ["int", "bigint", "mediumint"]:
        mgr._get_column_data_type = mock.MagicMock(return_value=data_type)
        part_opts = mgr._define_native_column_read_partitioning(
            "table1",
            "column",
            ["int", "bigint", "mediumint"],
            99,
            50,
            "test column",
            None,
        )
        assert part_opts == {
            SPARK_PARTITION_COLUMN: "column",
            SPARK_NUM_PARTITIONS: 2,
            SPARK_LOWER_BOUND: 1,
            SPARK_UPPER_BOUND: 100,
            PARTITION_COMMENT: f"Partitioning by {data_type} test column",
        }


def test_read_partitioning_df():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    table_list = ["table1", "table2", "table3"]
    mgr.set_table_list(table_list)
    read_partitioning_dict = {
        "table2": {
            "partitionColumn": "ID",
            "numPartitions": 15,
            "lowerBound": 1,
            "upperBound": 444,
        }
    }

    df = mgr.read_partitioning_df(read_partitioning_dict)

    assert isinstance(df, pd.DataFrame)
    assert len(df["table"]) == 3
    assert "table1" in list(df["table"])
    assert "table2" in list(df["table"])
    assert "table3" in list(df["table"])
    table2_row = list(df["table"]).index("table2")
    assert list(df["partition_column"])[table2_row] == "ID"
    assert list(df["num_partitions"])[table2_row] == 15
    assert list(df["lower_bound"])[table2_row] == 1
    assert list(df["upper_bound"])[table2_row] == 444


def test__get_count_sql():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    assert isinstance(mgr._get_count_sql("TABLE"), str)


def test__get_max_sql():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    assert isinstance(mgr._get_max_sql("TABLE", "COLUMN"), str)


def test__get_min_sql():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    assert isinstance(mgr._get_min_sql("TABLE", "COLUMN"), str)


def test__normalise_oracle_data_type():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    for oracle_type, normalised_type in [
        ("DATE", "DATE"),
        ("TIMESTAMP(0)", "TIMESTAMP"),
        ("TIMESTAMP(3) WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE"),
        ("TIMESTAMP(6) WITH LOCAL TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"),
        ("INTERVAL DAY(5) TO SECOND(1)", "INTERVAL DAY TO SECOND"),
    ]:
        assert mgr._normalise_oracle_data_type(oracle_type) == normalised_type


def test__read_partitioning_num_partitions():
    mgr = JDBCInputManager.create("oracle", ALCHEMY_DB)
    # Numeric ranges
    for num_rows, stride, expected_partitions in [
        [1, 10, 1],
        [100, 10, 10],
        [float(100), float(10), 10],
        [Decimal(100), Decimal(10), 10],
        [int(99), Decimal(10), 10],
        [
            Decimal(9_999_999_999_999_999_999),
            Decimal(1_000_000_000_000_000_000),
            10,
        ],
        [105, 10, 11],
        [0, 10, 1],
    ]:
        assert (
            mgr._read_partitioning_num_partitions(num_rows, stride)
            == expected_partitions
        )
