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

from typing import Optional, List, Any

from pyspark.sql import DataFrame, SparkSession
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