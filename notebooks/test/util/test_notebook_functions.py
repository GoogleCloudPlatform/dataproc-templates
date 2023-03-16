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

from util.notebook_functions import (
    split_list,
    remove_unexpected_spanner_primary_keys,
    update_spanner_primary_keys,
)


def test_split_list():
    for input_list, split_size, expected_outcome in [
        (
            [1, 2, 3, 4, 5, 6],
            3,
            [[1, 2, 3], [4, 5, 6]],
        ),
        (
            ["tab1", "tab2", "tab3", "tab4", "tab5", "tab6"],
            3,
            [["tab1", "tab2", "tab3"], ["tab4", "tab5", "tab6"]],
        ),
        (
            [1, 2, 3, 4, 5, 6],
            2,
            [[1, 2], [3, 4], [5, 6]],
        ),
        (
            [1, 2, 3, 4, 5, 6],
            7,
            [[1, 2, 3, 4, 5, 6]],
        ),
        (
            [1],
            7,
            [[1]],
        ),
        (
            [],
            7,
            [],
        ),
    ]:
        outcome = split_list(input_list, split_size)
        assert outcome == expected_outcome


def test_update_spanner_primary_keys():
    spanner_pk_dict = {
        "TaBlE2": "col3,col4",
    }

    table_name, source_pk_columns = "table1", ["col1", "col2"]
    update_spanner_primary_keys(spanner_pk_dict, table_name, source_pk_columns)
    assert "table1" in spanner_pk_dict
    assert spanner_pk_dict["table1"] == "col1,col2"

    table_name, source_pk_columns = "table2", ["col2"]
    update_spanner_primary_keys(spanner_pk_dict, table_name, source_pk_columns)
    assert "TaBlE2" not in spanner_pk_dict
    assert "table2" in spanner_pk_dict
    assert spanner_pk_dict["table2"] == "col3,col4"

    table_name, source_pk_columns = "table3", []
    update_spanner_primary_keys(spanner_pk_dict, table_name, source_pk_columns)
    assert "table3" in spanner_pk_dict
    assert spanner_pk_dict["table3"] == ""


def test_remove_unexpected_spanner_primary_keys():
    spanner_pk_dict = {
        "TABLE1": "col1,col2",
        "TABLE2": "col3,col4",
        "TABLE3": "col1,col2",
        "TABLE4": "",
        "TABLE5": None,
    }
    expected_names = ["TABLE1", "TABLE2"]
    remove_unexpected_spanner_primary_keys(spanner_pk_dict, expected_names)
    assert "TABLE1" in spanner_pk_dict
    assert spanner_pk_dict["TABLE1"] == "col1,col2"
    assert "TABLE2" in spanner_pk_dict
    assert spanner_pk_dict["TABLE2"] == "col3,col4"
    assert "TABLE3" not in spanner_pk_dict
    assert "TABLE4" not in spanner_pk_dict
    assert "TABLE5" not in spanner_pk_dict
    assert len(spanner_pk_dict) == 2
