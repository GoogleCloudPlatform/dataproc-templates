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

spark = SparkSession.builder.appName("BigQuery to GCS load").getOrCreate()
spark.sparkContext.setLogLevel("INFO")

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

def runTemplate():

  # Arguments
  args = parse_args()
  project_id            = args[PROJECT_ID_PROP]
  inputTableName        = args[BQ_GCS_INPUT_TABLE_NAME]
  outputFileFormat      = args[BQ_GCS_OUTPUT_FORMAT]
  outputMode            = args[BQ_GCS_OUTPUT_MODE]
  outputFileLocation    = args[BQ_GCS_OUTPUT_LOCATION]

  # Read
  inputData = spark.read.format("bigquery").option("table",inputTableName).load()

  logger.info("Starting Bigquery to GCS spark job with following parameters\n 1. {}={}\n 2. {}={}\n 3. {}={}\n 4. {}={}\n 5. {}={}\n".format(
              PROJECT_ID_PROP,
              project_id,
              BQ_GCS_INPUT_TABLE_NAME,
              inputTableName,
              BQ_GCS_OUTPUT_FORMAT,
              outputFileFormat,
              BQ_GCS_OUTPUT_MODE,
              outputMode,
              BQ_GCS_OUTPUT_LOCATION,
              outputFileLocation)
  )

  # Write
  if (outputFileFormat == BQ_GCS_OUTPUT_FORMAT_PARQUET):
      if (outputMode == BQ_GCS_OUTPUT_MODE_OVERWRITE):
          inputData.write.mode(BQ_GCS_OUTPUT_MODE_OVERWRITE).parquet(outputFileLocation)
      elif (outputMode == BQ_GCS_OUTPUT_MODE_APPEND):
          inputData.write.mode(BQ_GCS_OUTPUT_MODE_APPEND).parquet(outputFileLocation)
      else:
          raise Exception("Only Append and Overwrite modes are supported")

  elif (outputFileFormat == BQ_GCS_OUTPUT_FORMAT_AVRO):
      if (outputMode == BQ_GCS_OUTPUT_MODE_OVERWRITE):
          inputData.write.mode(BQ_GCS_OUTPUT_MODE_OVERWRITE).format(GCS_BQ_AVRO_EXTD_FORMAT).save(outputFileLocation)
      elif (outputMode == BQ_GCS_OUTPUT_MODE_APPEND):
          inputData.write.mode(BQ_GCS_OUTPUT_MODE_APPEND).format(GCS_BQ_AVRO_EXTD_FORMAT).save(outputFileLocation)
      else:
          raise Exception("Only Append and Overwrite modes are supported")

  elif (outputFileFormat == BQ_GCS_OUTPUT_FORMAT_CSV):
      if (outputMode == BQ_GCS_OUTPUT_MODE_OVERWRITE):
          inputData.write.mode(BQ_GCS_OUTPUT_MODE_OVERWRITE).csv(outputFileLocation)
      elif (outputMode == BQ_GCS_OUTPUT_MODE_APPEND):
          inputData.write.mode(BQ_GCS_OUTPUT_MODE_APPEND).csv(outputFileLocation)
      else:
          raise Exception("Only Append and Overwrite modes are supported")

  elif (outputFileFormat == BQ_GCS_OUTPUT_FORMAT_JSON):
      if (outputMode == BQ_GCS_OUTPUT_MODE_OVERWRITE):
          inputData.write.mode(BQ_GCS_OUTPUT_MODE_OVERWRITE).json(outputFileLocation)
      elif (outputMode == BQ_GCS_OUTPUT_MODE_APPEND):
          inputData.write.mode(BQ_GCS_OUTPUT_MODE_APPEND).json(outputFileLocation)
      else:
          raise Exception("Only Append and Overwrite modes are supported")

  else:
    raise Exception("Currently avro, parquet, csv and json are the only supported formats")

def parse_args():

  config = configparser.ConfigParser()
  config.read('default_args.ini')
  defaults = config['bigquery_to_gcs']
  commons =  config['common']

  parser = argparse.ArgumentParser()
  parser.add_argument('--'+PROJECT_ID_PROP, dest=PROJECT_ID_PROP)
  parser.add_argument('--'+BQ_GCS_INPUT_TABLE_NAME, dest=BQ_GCS_INPUT_TABLE_NAME)
  parser.add_argument('--'+BQ_GCS_OUTPUT_FORMAT, dest=BQ_GCS_OUTPUT_FORMAT)
  parser.add_argument('--'+BQ_GCS_OUTPUT_MODE, dest=BQ_GCS_OUTPUT_MODE)
  parser.add_argument('--'+BQ_GCS_OUTPUT_LOCATION, dest=BQ_GCS_OUTPUT_LOCATION)

  args = vars(parser.parse_args())
  result = {**dict(defaults),**dict(commons)}
  result.update({k: v for k, v in args.items() if v is not None})

  if (None in list(result.values())) or ("" in list(result.values())):
    raise Exception("Required parameters for BigQuery to GCS not passed")

  return result


if __name__ == '__main__':
  runTemplate()