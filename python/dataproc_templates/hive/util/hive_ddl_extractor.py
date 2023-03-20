#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import pprint
from logging import Logger
from typing import Any, Dict, Optional, Sequence
import dataproc_templates.util.template_constants as constants
from dataproc_templates import BaseTemplate
from pyspark.sql import SparkSession
from datetime import datetime

class HiveDDLExtractorTemplate(BaseTemplate):
    """
    Dataproc template implementing exports from Hive to BigQuery
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.HIVE_DDL_EXTRACTOR_INPUT_DATABASE}',
            dest=constants.HIVE_DDL_EXTRACTOR_INPUT_DATABASE,
            required=True,
            help='Hive database for importing data to BigQuery'
        )

        parser.add_argument(
            f'--{constants.HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH}',
            dest=constants.HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH,
            required=True,
            help='GCS output path'
        )

        parser.add_argument(
            f'--{constants.HIVE_DDL_CONSIDER_SPARK_TABLES}',
            dest=constants.HIVE_DDL_CONSIDER_SPARK_TABLES,
            required=False,
            default=False,
            help='Flag to extract DDL of Spark tables'
        )

        parser.add_argument(
            f'--{constants.HIVE_DDL_TRANSLATION_DISPOSITION}',
            dest=constants.HIVE_DDL_TRANSLATION_DISPOSITION,
            required=False,
            default=False,
            help='Remove location parameter from HIVE DDL if set to TRUE, to be compatible with BigQuery SQL translator'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)


    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:
        """
        
        Dataproc template allowing the extraction of DDLs from Hive Metastore

        """
        logger: Logger = self.get_logger(spark=spark)

        hive_database: str = args[constants.HIVE_DDL_EXTRACTOR_INPUT_DATABASE]
        gcs_output_path: str = args[constants.HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH]
        spark_tbls_flag: bool = args[constants.HIVE_DDL_CONSIDER_SPARK_TABLES]
        remove_location_flag: bool = args[constants.HIVE_DDL_TRANSLATION_DISPOSITION]

        logger.info(
            "Starting Hive DDL Extraction job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        def get_ddl(hive_database, table_name):
            spark_tbls_opt = "" if str(spark_tbls_flag).upper() == "TRUE" else "AS SERDE"
            ddl_str = spark.sql(f"SHOW CREATE TABLE {hive_database}.{table_name} {spark_tbls_opt}").rdd.map(lambda x: x[0]).collect()[0]
            ddl_str = ddl_str + ";" if str(remove_location_flag).upper() != "TRUE" else ddl_str.split("\nLOCATION '")[0].split("\nUSING ")[0]+";"
            return ddl_str

        ct = datetime.now().strftime("%m-%d-%Y %H.%M.%S")
        output_path = gcs_output_path+"/"+hive_database+"/"+str(ct)

        tables_names = spark.sql(f"SHOW TABLES IN {hive_database}").select("tableName")
        tables_name_list = tables_names.rdd.map(lambda x: x[0]).collect()
        tables_ddls = [get_ddl(hive_database, table_name) for table_name in tables_name_list]

        ddls_rdd = spark.sparkContext.parallelize(tables_ddls)
        ddls_rdd.coalesce(1).saveAsTextFile(output_path)
