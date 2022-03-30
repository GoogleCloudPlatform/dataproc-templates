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

from pyspark.sql import SparkSession
import argparse, configparser

from template_constants import *

spark = SparkSession.builder.appName("GCS to Bigquery load").getOrCreate()
spark.sparkContext.setLogLevel("INFO")

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

def runTemplate():

  # Arguments
  args = parse_args()
  input_file_location = args[GCS_BQ_INPUT_LOCATION]
  big_query_dataset   = args[GCS_OUTPUT_DATASET_NAME]
  big_query_table     = args[GCS_OUTPUT_TABLE_NAME]
  input_file_format   = args[GCS_BQ_INPUT_FORMAT]
  bq_temp_bucket      = args[GCS_BQ_LD_TEMP_BUCKET_NAME]

  # Read
  if (input_file_format == GCS_BQ_PRQT_FORMAT):
    input_data = spark.read \
                      .parquet(input_file_location)
  elif (input_file_format == GCS_BQ_AVRO_FORMAT):
    input_data = spark.read \
                      .format(GCS_BQ_AVRO_EXTD_FORMAT) \
                      .load(input_file_location)
  elif (input_file_format == GCS_BQ_CSV_FORMAT):
    input_data = spark.read \
                      .format(GCS_BQ_CSV_FORMAT) \
                      .option(GCS_BQ_CSV_HEADER, True) \
                      .option(GCS_BQ_CSV_INFER_SCHEMA, True) \
                      .load(input_file_location)
  else:
    raise Exception("Currently avro, parquet and csv are the only supported formats")

  logger.info("Starting GCS to Bigquery spark job with following parameters\n 1. {}={}\n 2. {}={}\n 3. {}={}\n 4. {}={}\n 5. {}={}\n".format(
              GCS_BQ_INPUT_LOCATION,
              input_file_location,
              GCS_OUTPUT_DATASET_NAME,
              big_query_dataset,
              GCS_OUTPUT_TABLE_NAME,
              big_query_table,
              GCS_BQ_INPUT_FORMAT,
              input_file_format,
              GCS_BQ_LD_TEMP_BUCKET_NAME,
              bq_temp_bucket)
  )

  # Write
  input_data.write \
            .format(GCS_BQ_OUTPUT_FORMAT) \
            .option(GCS_BQ_OUTPUT, big_query_dataset + "." + big_query_table) \
            .option(GCS_BQ_TEMP_BUCKET, bq_temp_bucket) \
            .mode("append") \
            .save()

def parse_args():

  config = configparser.ConfigParser()
  config.read('default_args.ini')
  defaults = config['gcs_to_bigquery']

  parser = argparse.ArgumentParser()
  parser.add_argument('--'+GCS_BQ_INPUT_LOCATION, dest=GCS_BQ_INPUT_LOCATION)
  parser.add_argument('--'+GCS_OUTPUT_DATASET_NAME, dest=GCS_OUTPUT_DATASET_NAME)
  parser.add_argument('--'+GCS_OUTPUT_TABLE_NAME, dest=GCS_OUTPUT_TABLE_NAME)
  parser.add_argument('--'+GCS_BQ_INPUT_FORMAT, dest=GCS_BQ_INPUT_FORMAT)
  parser.add_argument('--'+GCS_BQ_LD_TEMP_BUCKET_NAME, dest=GCS_BQ_LD_TEMP_BUCKET_NAME)

  args = vars(parser.parse_args())
  result = dict(defaults)
  result.update({k: v for k, v in args.items() if v is not None})

  if (None in list(result.values())) or ("" in list(result.values())):
    raise Exception("Required parameters for gcs_to_bigquery not passed")

  return result

if __name__ == '__main__':
  runTemplate()