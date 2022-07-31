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


from typing import Dict, Any, Type, Sequence, Optional
import logging
import argparse
import sys
import dataproc_templates.util.template_constants as constants
from pyspark.sql import SparkSession


LOGGER: logging.Logger = logging.getLogger('dataproc_templates')



def create_spark_session() -> SparkSession:
    """
    Creates the SparkSession object.

    It also sets the Spark logging level to info. We could
    consider parametrizing the log level in the future.

    Args:
        template_name (str): The name of the template being
            run. Used to set the Spark app name.

    Returns:
        pyspark.sql.SparkSession: The set up SparkSession.
    """

    spark = SparkSession.builder \
        .appName("GetTablesList") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    return spark


def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    parser: argparse.ArgumentParser = argparse.ArgumentParser()

    parser.add_argument(
        f'--{constants.HIVE_BQ_INPUT_DATABASE}',
        dest=constants.HIVE_BQ_INPUT_DATABASE,
        required=True,
        help='Hive database for importing data to BigQuery'
    )

    parser.add_argument(
        f'--{constants.HIVE_BQ_OUTPUT_DATASET}',
        dest=constants.HIVE_BQ_OUTPUT_DATASET,
        required=True,
        help='BigQuery dataset for the output table'
    )

    parser.add_argument(
        f'--{constants.HIVE_BQ_LD_TEMP_BUCKET_NAME}',
        dest=constants.HIVE_BQ_LD_TEMP_BUCKET_NAME,
        required=True,
        help='Spark BigQuery connector temporary bucket'
    )

    parser.add_argument(
        f'--{constants.HIVE_BQ_OUTPUT_MODE}',
        dest=constants.HIVE_BQ_OUTPUT_MODE,
        required=False,
        default=constants.OUTPUT_MODE_APPEND,
        help=(
            'Output write mode '
            '(one of: append,overwrite,ignore,errorifexists) '
            '(Defaults to append)'
        ),
        choices=[
            constants.OUTPUT_MODE_OVERWRITE,
            constants.OUTPUT_MODE_APPEND,
            constants.OUTPUT_MODE_IGNORE,
            constants.OUTPUT_MODE_ERRORIFEXISTS
        ]
    )

    known_args: argparse.Namespace
    known_args, _ = parser.parse_known_args(args)

    return vars(known_args)

def run_template() -> None:
    """
    Executes a template given it's template name.

    Args:
        template_name (TemplateName): The TemplateName of the template
            that should be run.

    Returns:
        None
    """

    # pylint: disable=broad-except
#    LOGGER.info('Running template %s', template_name.value)

    try:
        args: Dict[str, Any] = parse_args()
        hive_database: str = args[constants.HIVE_BQ_INPUT_DATABASE]
        temp_bucket: str = args[constants.HIVE_BQ_LD_TEMP_BUCKET_NAME]
        spark: SparkSession = create_spark_session()
        df=spark.sql("show tables in {}".format(hive_database))
        df.select("tableName").coalesce(1).write.mode("overwrite").csv("gs://"+temp_bucket+"/"+hive_database)

    except Exception:
        LOGGER.exception(
            'An error occurred while running %s template'
        )
        sys.exit(1)


if __name__ == '__main__':
    LOGGER.setLevel(logging.INFO)

    run_template()
