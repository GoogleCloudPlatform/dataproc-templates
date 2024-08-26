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

from typing import Optional

from pyspark.sql import DataFrame, SparkSession

import dataproc_templates.util.template_constants as constants
from dataproc_templates.util.elasticsearch_transformations import rename_columns


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
        "es.net.http.auth.pass": es_password,
        "es.output.json": "true"
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
