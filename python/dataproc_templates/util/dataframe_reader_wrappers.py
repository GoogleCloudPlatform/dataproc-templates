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

from typing import Optional, Dict, Any
import re
import json 

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, ArrayType

import dataproc_templates.util.template_constants as constants


def ingest_dataframe_from_cloud_storage(
    spark: SparkSession,
    args: dict,
    input_location: str,
    input_format: str,
    prefix: str,
    avro_format_override: Optional[str] = None,
) -> DataFrame:
    """Return a Dataframe reader object with methods and options applied for reading from Cloud Storage."""
    input_data: DataFrame

    csv_input_constant_options: dict = constants.get_csv_input_spark_options(prefix)
    spark_options = {csv_input_constant_options[k]: v
                     for k, v in args.items()
                     if k in csv_input_constant_options and v}

    if input_format == constants.FORMAT_PRQT:
        input_data = spark.read \
            .parquet(input_location)
    elif input_format == constants.FORMAT_AVRO:
        input_data = spark.read \
            .format(avro_format_override or constants.FORMAT_AVRO_EXTD) \
            .load(input_location)
    elif input_format == constants.FORMAT_CSV:
        input_data = spark.read \
            .format(constants.FORMAT_CSV) \
            .options(**spark_options) \
            .load(input_location)
    elif input_format == constants.FORMAT_JSON:
        input_data = spark.read \
            .json(input_location)
    elif input_format == constants.FORMAT_DELTA:
        input_data = spark.read \
            .format(constants.FORMAT_DELTA) \
            .load(input_location)

    return input_data


def rename_duplicate_columns(
    dataframe_schema: Dict[str, Any], 
    column_name_set: set = set(), 
    parent: tuple = ()
) -> Dict[str, Any]:
    """Return a modified dataframe schema dict with the duplicate columns renamed"""

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


def ingest_dataframe_from_elasticsearch(
    spark: SparkSession,
    es_node: str, 
    es_index: str, 
    es_user: str, 
    es_password: str,
    args: dict,
    prefix: str,
) -> DataFrame:
    """Return a Dataframe reader object with methods and options applied for reading from Cloud Storage."""
    input_data: DataFrame

    es_spark_connector_input_options: dict = constants.get_es_spark_connector_input_options(prefix)
    es_spark_connector_options = {es_spark_connector_input_options[k]: v
                     for k, v in args.items()
                     if k in es_spark_connector_input_options and v}

    # Making Spark Case Sensitive
    spark.conf.set('spark.sql.caseSensitive', True)

    es_conf_json = {
        "es.nodes": es_node,
        "es.resource": es_index,
        "es.net.http.auth.user": es_user,
        "es.net.http.auth.pass": es_password
    }

    # Merging the Required and Optional attributes
    es_conf_json.update(es_spark_connector_options)

    # Read as RDD
    input_data = spark.sparkContext.newAPIHadoopRDD(constants.FORMAT_ELASTICSEARCH,\
        constants.ELASTICSEARCH_KEY_CLASS,\
        constants.ELASTICSEARCH_VALUE_CLASS,\
        conf=es_conf_json)
    
    # Remove the Elasticsearch ID from the RDD
    input_data = input_data.flatMap(lambda x: x[1:])

    # Convert into Dataframe 
    input_data = spark.read.json(input_data)

    # Remove Special Characters from the Column Names
    input_data = rename_columns(input_data)

    return input_data
