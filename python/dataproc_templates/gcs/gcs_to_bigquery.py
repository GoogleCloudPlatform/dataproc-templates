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

from typing import Dict

from pyspark.sql import SparkSession

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['GcsToBigQueryTemplate']


class GcsToBigQueryTemplate(BaseTemplate):
    
    def run(self, properties: Dict[str, str]) -> None:
        spark = SparkSession.builder\
            .appName("GCS to Bigquery load") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("INFO")

        log4jLogger = spark.sparkContext._jvm.org.apache.log4j
        logger = log4jLogger.LogManager.getLogger(__name__)

        # Arguments
        input_file_location = properties[constants.GCS_BQ_INPUT_LOCATION]
        big_query_dataset = properties[constants.GCS_BQ_OUTPUT_DATASET]
        big_query_table = properties[constants.GCS_BQ_OUTPUT_TABLE]
        input_file_format = properties[constants.GCS_BQ_INPUT_FORMAT]
        bq_temp_bucket = properties[constants.GCS_BQ_LD_TEMP_BUCKET_NAME]

        # Read
        if (input_file_format == constants.GCS_BQ_PRQT_FORMAT):
            input_data = spark.read \
                                .parquet(input_file_location)
        elif (input_file_format == constants.GCS_BQ_AVRO_FORMAT):
            input_data = spark.read \
                                .format(constants.GCS_BQ_AVRO_EXTD_FORMAT) \
                                .load(input_file_location)
        elif (input_file_format == constants.GCS_BQ_CSV_FORMAT):
            input_data = spark.read \
                                .format(constants.GCS_BQ_CSV_FORMAT) \
                                .option(constants.GCS_BQ_CSV_HEADER, True) \
                                .option(constants.GCS_BQ_CSV_INFER_SCHEMA, True) \
                                .load(input_file_location)
        else:
            raise Exception("Currently avro, parquet and csv are the only supported formats")

        logger.info("Starting GCS to Bigquery spark job with following parameters\n 1. {}={}\n 2. {}={}\n 3. {}={}\n 4. {}={}\n 5. {}={}\n".format(
                    constants.GCS_BQ_INPUT_LOCATION,
                    input_file_location,
                    constants.GCS_BQ_OUTPUT_DATASET,
                    big_query_dataset,
                    constants.GCS_BQ_OUTPUT_TABLE,
                    big_query_table,
                    constants.GCS_BQ_INPUT_FORMAT,
                    input_file_format,
                    constants.GCS_BQ_LD_TEMP_BUCKET_NAME,
                    bq_temp_bucket)
        )

        # Write
        input_data.write \
                .format(constants.GCS_BQ_OUTPUT_FORMAT) \
                .option(constants.GCS_BQ_OUTPUT, big_query_dataset + "." + big_query_table) \
                .option(constants.GCS_BQ_TEMP_BUCKET, bq_temp_bucket) \
                .mode("append") \
                .save()
