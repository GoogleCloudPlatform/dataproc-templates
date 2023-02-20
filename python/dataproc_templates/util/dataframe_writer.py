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


def persist_dataframe_to_cloud_storage(
    input_dataframe: DataFrame,
    file_format: str,
    file_location: str,
    spark_options: dict,
) -> DataFrame:
    """Persist input_dataframe object with methods and options applied for writing to Cloud Storage."""
    if file_format == constants.FORMAT_PRQT:
        input_dataframe \
            .parquet(file_location)
    elif file_format == constants.FORMAT_AVRO:
        input_dataframe \
            .format(constants.FORMAT_AVRO) \
            .save(file_location)
    elif file_format == constants.FORMAT_CSV:
        input_dataframe \
            .options(**spark_options) \
            .csv(file_location)
    elif file_format == constants.FORMAT_JSON:
        input_dataframe \
            .json(file_location)
