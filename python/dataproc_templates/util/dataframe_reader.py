# Copyright 2022 Google LLC
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


def get_gcs_reader(
    spark: SparkSession,
    file_format: str,
    file_location: str,
    spark_options: dict,
    avro_format_override: Optional[str] = None
) -> DataFrame:
    """Return a Dataframe reader object with methods and options applied for reading from GCS."""
    input_data: DataFrame

    if file_format == constants.FORMAT_PRQT:
        input_data = spark.read \
            .parquet(file_location)
    elif file_format == constants.FORMAT_AVRO:
        input_data = spark.read \
            .format(avro_format_override or constants.FORMAT_AVRO_EXTD) \
            .load(file_location)
    elif file_format == constants.FORMAT_CSV:
        input_data = spark.read \
            .format(constants.FORMAT_CSV) \
            .options(**spark_options) \
            .load(file_location)
    elif file_format == constants.FORMAT_JSON:
        input_data = spark.read \
            .json(file_location)
    elif file_format == constants.FORMAT_DELTA:
        input_data = spark.read \
            .format(constants.FORMAT_DELTA) \
            .load(file_location)

    return input_data
