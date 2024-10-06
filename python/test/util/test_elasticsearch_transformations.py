"""
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.testing.utils import assertDataFrameEqual

from dataproc_templates.util.elasticsearch_transformations import \
    flatten_struct_fields, \
    detect_multidimensional_array_columns, \
    flatten_array_fields, \
    rename_duplicate_columns, \
    modify_json_schema, \
    rename_columns

@pytest.fixture
def spark_session():
    """Spark Session Fixture"""
    spark = SparkSession.builder \
        .appName("Spark-Test-Application") \
        .enableHiveSupport() \
        .getOrCreate()

    yield spark

def test_flatten_struct_fields(spark_session):
    """Test flatten struct fields function"""
    nested_schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("nested", StructType([
                    StructField("field1", StringType(), True),
                    StructField("field2", StringType(), True)
                ]), True)
            ])

    nested_data = [(1, ("data1", "data2")), (2, ("data3", "data4"))]

    # Create a Spark DataFrame
    original_df = spark_session.createDataFrame(nested_data, nested_schema)

    # Apply the transformation function from before
    transformed_df = flatten_struct_fields(original_df)

    expected_schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("nested__field1", StringType(), True),
                StructField("nested__field2", StringType(), True)
            ])

    expected_data = [(1, "data1", "data2"), (2, "data3", "data4")]

    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    assertDataFrameEqual(transformed_df, expected_df)

def test_detect_multidimensional_array_columns(spark_session):
    """Test detect multidimentional array columns function"""
    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("contact_numbers", ArrayType(StringType()), nullable=False),
        StructField("attributes", ArrayType(ArrayType(StringType())), nullable=False),
        StructField("properties", ArrayType(ArrayType(ArrayType(IntegerType()))), nullable=False)
    ])

    df = spark_session.createDataFrame([], schema)

    output = detect_multidimensional_array_columns(df)

    expected_result = [("attributes", 2), ("properties", 3)]

    assert output == expected_result

def test_flatten_array_fields(spark_session):
    """Test flatten array fields function"""
    schema = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("matrix-2d", ArrayType(ArrayType(IntegerType())), nullable=False),
            StructField("matrix-3d", ArrayType(ArrayType(ArrayType(IntegerType()))), nullable=False)
        ])
    df = spark_session.createDataFrame([(1, [[1, 2], [3, 4]], [[[1, 2]], [[3,4]]])], schema)
    flattened_df = flatten_array_fields(df)

    # Check the new schema has the same root level fields with updated array depth
    assert len(flattened_df.schema.fields) == len(df.schema.fields)
    assert isinstance(flattened_df.schema.fields[1].dataType, ArrayType)
    assert not isinstance(flattened_df.schema.fields[1].dataType.elementType, ArrayType)
    assert isinstance(flattened_df.schema.fields[2].dataType, ArrayType)
    assert not isinstance(flattened_df.schema.fields[2].dataType.elementType, ArrayType)

    # Check that the actual data has been flattened correctly
    expected_data = [(1, [1, 2, 3, 4], [1, 2, 3, 4])]
    assert flattened_df.collect() == expected_data

def test_rename_duplicate_columns():
    """Test renam duplicate columns function"""
    schema = {
    "fields": [
        {"metadata": {}, "name": "@timestamp", "nullable": True, "type": "long"},
        {
            "metadata": {},
            "name": "user",
            "nullable": True,
            "type": {
                "fields": [
                    {
                        "metadata": {},
                        "name": "name",
                        "nullable": True,
                        "type": "string",
                    },
                    {
                        "metadata": {},
                        "name": "name",
                        "nullable": True,
                        "type": "string",
                    },
                ],
                "type": "struct",
            },
        },
    ],
    "type": "struct",
}

    expected_schema = {
    "fields": [
        {"metadata": {}, "name": "@timestamp", "nullable": True, "type": "long"},
        {
            "metadata": {},
            "name": "user",
            "nullable": True,
            "type": {
                "fields": [
                    {
                        "metadata": {},
                        "name": "name",
                        "nullable": True,
                        "type": "string",
                    },
                    {
                        "metadata": {},
                        "name": "name_1",
                        "nullable": True,
                        "type": "string",
                    },
                ],
                "type": "struct",
            },
        },
    ],
    "type": "struct",
}


    assert rename_duplicate_columns(schema) == expected_schema

def test_modify_json_schema():
    """Test modify json schema function"""
    schema = {
    "fields": [
        {"metadata": {}, "name": "@timestamp", "nullable": True, "type": "long"},
        {
            "metadata": {},
            "name": "check",
            "nullable": True,
            "type": {
                "fields": [
                    {
                        "metadata": {},
                        "name": "test!id",
                        "nullable": True,
                        "type": "string",
                    },
                    {
                        "metadata": {},
                        "name": "type.test",
                        "nullable": True,
                        "type": "string",
                    },
                ],
                "type": "struct",
            },
        },
        {"metadata": {}, "name": "testing-test", "nullable": True, "type": "double"},
    ],
    "type": "struct",
}

    expected_schema = {
    "fields": [
        {"metadata": {}, "name": "_timestamp", "nullable": True, "type": "long"},
        {
            "metadata": {},
            "name": "check",
            "nullable": True,
            "type": {
                "fields": [
                    {
                        "metadata": {},
                        "name": "test_id",
                        "nullable": True,
                        "type": "string",
                    },
                    {
                        "metadata": {},
                        "name": "type_test",
                        "nullable": True,
                        "type": "string",
                    },
                ],
                "type": "struct",
            },
        },
        {"metadata": {}, "name": "testing_test", "nullable": True, "type": "double"},
    ],
    "type": "struct",
}


    assert modify_json_schema(schema) == expected_schema

def test_rename_columns(spark_session):
    """Test rename columns function"""
    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("person-info", StructType([
            StructField("first@name", StringType(), nullable=False),
            StructField("last$name", StringType(), nullable=False),
            StructField("last name", StringType(), nullable=False)
        ]), nullable=False)
    ])
    data = [(1, ("Alice", "Smith", "Smith")), (2, ("Bob", "Johnson", "Johnson"))]
    df = spark_session.createDataFrame(data, schema)

    renamed_df = rename_columns(df)
    expected_columns = ["id", "person_info"]
    expected_inner_columns = ["first_name", "last_name", "last_name_1"]

    assert renamed_df.columns == expected_columns
    assert all(
        col.name in expected_inner_columns for col in renamed_df.schema["person_info"].dataType.fields
    )
