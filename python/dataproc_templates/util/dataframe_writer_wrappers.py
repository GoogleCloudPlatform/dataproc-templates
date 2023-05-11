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

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

import dataproc_templates.util.template_constants as constants


def persist_dataframe_to_cloud_storage(
    input_dataframe: DataFrame,
    args: dict,
    output_location: str,
    output_format: str,
    prefix: str,
) -> DataFrame:
    """Persist input_dataframe object with methods and options applied for writing to Cloud Storage."""

    csv_output_constant_options: dict = constants.get_csv_output_spark_options(prefix)
    spark_options = {csv_output_constant_options[k]: v for k, v in args.items() if k in csv_output_constant_options and v}

    if output_format == constants.FORMAT_PRQT:
        input_dataframe \
            .parquet(output_location)
    elif output_format == constants.FORMAT_AVRO:
        input_dataframe \
            .format(constants.FORMAT_AVRO) \
            .save(output_location)
    elif output_format == constants.FORMAT_CSV:
        input_dataframe \
            .options(**spark_options) \
            .csv(output_location)
    elif output_format == constants.FORMAT_JSON:
        input_dataframe \
            .json(output_location)

    return input_dataframe


def persist_streaming_dataframe_to_cloud_storage(
    datastream_writer: DataStreamWriter,
    args: dict,
    checkpoint_location: str,
    output_location: str,
    output_format: str,
    output_mode: str,
    prefix: str,
) -> DataStreamWriter:
    """Persist streaming input_dataframe object with methods and options applied for writing to Cloud Storage."""

    csv_output_constant_options: dict = constants.get_csv_output_spark_options(prefix)
    spark_options = {
        csv_output_constant_options[k]: v
        for k, v in args.items()
        if k in csv_output_constant_options and v
    }

    if output_format == constants.FORMAT_CSV:
        datastream_writer \
            .format(output_format) \
            .outputMode(output_mode) \
            .options(**spark_options) \
            .option(constants.STREAM_CHECKPOINT_LOCATION, checkpoint_location) \
            .option(constants.STREAM_PATH, output_location)
    elif output_format in (constants.FORMAT_JSON, constants.FORMAT_AVRO, constants.FORMAT_PRQT):
        datastream_writer \
            .format(output_format) \
            .outputMode(output_mode) \
            .option(constants.STREAM_CHECKPOINT_LOCATION, checkpoint_location) \
            .option(constants.STREAM_PATH, output_location)

    return datastream_writer
