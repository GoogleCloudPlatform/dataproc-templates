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


def WriteToCloud (ddls_to_rdd, bucket, path):
    # Write
    now = datetime.now()
    timedate = now.strftime("%m-%d-%Y %H.%M.%S")
    ddls_to_rdd.coalesce(1).saveAsTextFile("gs://"+bucket+"/"+path+timedate)

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
            f'--{constants.HIVE_DDL_EXTRACTOR_OUTPUT_BUCKET}',
            dest=constants.HIVE_DDL_EXTRACTOR_OUTPUT_BUCKET,
            required=True,
            help='Bucket for the output'
        )

        parser.add_argument(
            f'--{constants.HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH}',
            dest=constants.HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH,
            required=True,
            help='GCS output path'
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
        gcs_output_bucket: str = args[constants.HIVE_DDL_EXTRACTOR_OUTPUT_BUCKET]
        gcs_output_path: str = args[constants.HIVE_DDL_EXTRACTOR_OUTPUT_GCS_PATH]

        logger.info(
                "Starting Hive DDL Extraction job with parameters:\n"
                f"{pprint.pformat(args)}"
            )
        
               
        def get_ddl(hive_database, table_name):
            return spark.sql(f"SHOW CREATE TABLE {hive_database}.{table_name} as serde").rdd.map(lambda x: x[0] + ";").collect()[0]
        
        output_path="gs://"+gcs_output_bucket+"/"+gcs_output_path+"/"+hive_database+"/"+str(ct)
        tables_names = spark.sql(f"SHOW TABLES IN {hive_database}").select("tableName")
        tables_name_list=tables_names.rdd.map(lambda x: x[0]).collect()
        tables_ddls = [ get_ddl(hive_database, table_name) for table_name in tables_name_list ]
        ddls_rdd = spark.sparkContext.parallelize(tables_ddls)
        ct = datetime.now().strftime("%m-%d-%Y %H.%M.%S")
        ddls_rdd.coalesce(1).saveAsTextFile(output_path)