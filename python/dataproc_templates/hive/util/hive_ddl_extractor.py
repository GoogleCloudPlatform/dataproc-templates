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
from google.cloud import storage

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
            help='Remove location parameter from HIVE DDL if set to TRUE'
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
        gcs_bucket: str = gcs_output_path.split("gs://")[1].split("/")[0]
        gcs_output_path: str = gcs_output_path.split("gs://")[1].split("/",maxsplit=1)[1]

        logger.info(
                "Starting Hive DDL Extraction job with parameters:\n"
                f"{pprint.pformat(args)}"
            )
        
        def WriteToCloud(ddl, bucket, path, hive_database):
            """
            Write String as a file to GCS
            """
            print("Writing DDL to GCS: " + hive_database)
            client = storage.Client()
            bucket = client.get_bucket(bucket)
            blob = bucket.blob(path + "/" + hive_database + ".sql")
            blob.upload_from_string(ddl)


        def get_ddl(hive_database, table_name, spark_tbls_opt, remove_location_flag):
            ddl_str = spark.sql(f"SHOW CREATE TABLE {hive_database}.{table_name} {spark_tbls_opt}").rdd.map(lambda x: x[0] + ";").collect()[0]
            ddl = ddl_str if str(remove_location_flag).upper() != "TRUE" else ddl_str.split("\nLOCATION '")[0].split("\nUSING ")[0]
            return ddl

        spark_tbls_opt = "" if str(spark_tbls_flag).upper() == "TRUE" else "AS SERDE"
        tables_names = spark.sql(f"SHOW TABLES IN {hive_database}").select("tableName")
        tables_name_list = tables_names.rdd.map(lambda x: x[0]).collect()
        tables_ddls = [ get_ddl(hive_database, table_name, spark_tbls_opt, remove_location_flag) for table_name in tables_name_list ]
        table_ddls_str = ';\n'.join(tables_ddls) 
        WriteToCloud(table_ddls_str, gcs_bucket, gcs_output_path, hive_database)

