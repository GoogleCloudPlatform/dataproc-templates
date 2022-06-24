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

from typing import Dict, Sequence, Optional, Any
from logging import Logger
import argparse
import pprint

from pyspark.sql import SparkSession, DataFrame

from dataproc_templates import BaseTemplate
import dataproc_templates.util.template_constants as constants

__all__ = ['HbaseToBigtableTemplate']


class HbaseToBigtableTemplate(BaseTemplate):
    """
    Dataproc template implementing loads from Hbase into Bigtable
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()

        parser.add_argument(
            f'--{constants.HBASE_BIGTABLE_SOURCE_CATALOG_JSON}',
            dest=constants.HBASE_BIGTABLE_SOURCE_CATALOG_JSON,
            required=True,
            help='Hbase catalog json'
        )
        parser.add_argument(
            f'--{constants.HBASE_BIGTABLE_TARGET_CATALOG_JSON}',
            dest=constants.HBASE_BIGTABLE_TARGET_CATALOG_JSON,
            required=True,
            help='Bigtable catalog json'
        )

        known_args: argparse.Namespace
        known_args, _ = parser.parse_known_args(args)

        return vars(known_args)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:

        logger: Logger = self.get_logger(spark=spark)

        # Arguments
        hbase_catalog: str = ''.join(args[constants.HBASE_BIGTABLE_SOURCE_CATALOG_JSON].split())
        bigtable_catalog: str = ''.join(args[constants.HBASE_BIGTABLE_TARGET_CATALOG_JSON].split())

        logger.info(
            "Starting Hbase to Bigtable spark job with parameters:\n"
            f"{pprint.pformat(args)}"
        )

        # Read
        input_data: DataFrame
        input_data = spark.read.format(constants.FORMAT_HBASE) \
                           .options(catalog=hbase_catalog) \
                           .option("hbase.spark.use.hbasecontext", "false") \
                           .load()

        #write
            
        input_data.write \
            .format(constants.FORMAT_HBASE) \
            .options(catalog=bigtable_catalog) \
            .option('hbase.spark.use.hbasecontext', "false") \
            .option('hbase.client.connection.impl', "com.google.cloud.bigtable.hbase2_x.BigtableConnection") \
            .option('google.bigtable.project.id', "yadavaja-sandbox") \
            .option('google.bigtable.instance.id', "bt-templates-test") \
            .save()