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

from typing import Optional, Dict, List, Any

import re
import json

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, flatten
from pyspark.sql.types import StructType, ArrayType

def flatten_struct_fields(
    nested_df: DataFrame
) -> DataFrame:
    """Return a Dataframe with the struct columns flattened"""

    stack = [((), nested_df)]
    columns = []

    while stack:
        parents, df = stack.pop()

        flat_cols = [
            col(".".join(parents + (field.name,))).alias("__".join(parents + (field.name,)))
            for field in df.schema.fields
            if not isinstance(field.dataType, StructType)
        ]

        nested_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StructType)
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)


def detect_multidimensional_array_columns(
    df: DataFrame
) -> List[tuple]:
    """Return a Dataframe with the struct columns flattened"""

    multidim_columns = []
    for field in df.schema.fields:
        # Check if the field is an array
        if isinstance(field.dataType, ArrayType):
            depth = 0
            element_type = field.dataType
            # Unwrap ArrayTypes to find the depth and the innermost element type
            while isinstance(element_type, ArrayType):
                depth += 1
                element_type = element_type.elementType
            if depth > 1:
                multidim_columns.append((field.name, depth))
    return multidim_columns

def flatten_array_fields(
    df: DataFrame
) -> DataFrame:
    """Return a Dataframe with the multidimensional array columns flattened into one dimensional array columns"""

    columns_with_multidimensional_arrays = detect_multidimensional_array_columns(df)

    for column_name, depth in columns_with_multidimensional_arrays:
        while depth > 1:
            df = df.withColumn(column_name, flatten(col(column_name)))
            depth -= 1

    return df

def rename_duplicate_columns(
    dataframe_schema: Dict[str, Any],
    column_name_set: set = set(),
    parent: tuple = ()
) -> Dict[str, Any]:
    """Return a modified dataframe schema dict with the duplicate columns renamed"""
    if 'fields' in dataframe_schema:
        for fields in dataframe_schema['fields']:
            qualified_column_name = '.'.join(parent + (fields['name'],))
            new_qualified_column_name = qualified_column_name
            i = 1
            while new_qualified_column_name.lower() in column_name_set:
                new_qualified_column_name = f"{qualified_column_name}_{i}"
                i+=1
            column_name_set.add(new_qualified_column_name.lower())
            fields['name'] = new_qualified_column_name.split('.')[-1]

            if 'type' in fields and isinstance(fields['type'], dict):
                if fields['type']['type'] == "struct":
                    fields['type'] = rename_duplicate_columns(fields['type'], column_name_set, parent+(new_qualified_column_name.split('.')[-1],))

    return dataframe_schema


def modify_json_schema(
    dataframe_schema: Dict[str, Any]
) -> Dict[str, Any]:
    """Return a modified dataframe schema dict with the Special Characters replaced with _ in the column names"""

    if isinstance(dataframe_schema, dict):
        for key in list(dataframe_schema.keys()):
            if key == "name":
                # Replaces all non-alphanumeric characters with underscores
                dataframe_schema[key] = re.sub(r'[^a-zA-Z0-9_]+', '_', dataframe_schema[key])
            # Recur for nested dictionaries
            elif isinstance(dataframe_schema[key], dict):
                modify_json_schema(dataframe_schema[key])
            # Recur for each dictionary in the list if it's a list of dictionaries
            elif isinstance(dataframe_schema[key], list):
                for i in range(len(dataframe_schema[key])):
                    if isinstance(dataframe_schema[key][i], dict):
                        modify_json_schema(dataframe_schema[key][i])

    return dataframe_schema

def rename_columns(
    input_data: DataFrame,
) -> DataFrame:
    """Return a Dataframe with the Special Characters replaced with _ in the column names"""
    renamed_df: DataFrame

    # Rename the first level columns
    renamed_df = input_data.selectExpr(*[f"`{column}` as `{re.sub(r'[^a-zA-Z0-9_]+', '_', column)}`" for column in input_data.columns])

    # Rename the remaining columns
    json_schema = modify_json_schema(json.loads(renamed_df.schema.json()))

    # Rename the duplicate columns
    json_schema = rename_duplicate_columns(json_schema)

    replaced_schema = StructType.fromJson(json_schema)

    for col_schema in replaced_schema:
        if isinstance(col_schema.dataType, StructType) or isinstance(col_schema.dataType, ArrayType):
            renamed_df = renamed_df.withColumn(col_schema.name, renamed_df[col_schema.name].cast(col_schema.dataType))

    return renamed_df
