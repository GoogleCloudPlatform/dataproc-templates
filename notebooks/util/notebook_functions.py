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

from typing import List


def split_list(l: list, split_size: int) -> List[list]:
    """Split list l into sublists of max size split_size."""
    return_list = []
    for i in range(0, len(l), split_size):
        return_list.append(l[i : i + split_size])
    return return_list


def remove_unexpected_spanner_primary_keys(
    spanner_pk_dict: dict, expected_names: List[str]
):
    """
    Mutates spanner_pk_dict to removed unexpected entries, i.e. name NOT IN expected_names.
    """
    unknown_tables = [_ for _ in spanner_pk_dict if _ not in expected_names]
    if unknown_tables:
        print(f"Removing primary key entries for unknown tables: {unknown_tables}")
        for to_remove in unknown_tables:
            del spanner_pk_dict[to_remove]


def update_spanner_primary_keys(
    spanner_pk_dict: dict, table_name: str, source_pk_columns: list
):
    """
    Mutates spanner_pk_dict to reflect table_name: source_pk_columns.
    Also corrects case of table_name key is the user already provided a column list.
    """
    matching_key = [_ for _ in spanner_pk_dict if _.upper() == table_name.upper()]
    if matching_key:
        # Ensure user provided table_name is of correct case.
        matching_key = matching_key[0]
        if matching_key != table_name:
            spanner_pk_dict[table_name] = spanner_pk_dict[matching_key]
            del spanner_pk_dict[matching_key]
    else:
        spanner_pk_dict[table_name] = ",".join(source_pk_columns or "")
