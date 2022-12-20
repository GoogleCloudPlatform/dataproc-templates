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

from typing import Dict, Any, Type
import logging
import sys

from pyspark.sql import SparkSession

from dataproc_templates import BaseTemplate, TemplateName
from dataproc_templates.gcs.gcs_to_jdbc import GCSToJDBCTemplate
from dataproc_templates.mongo.mongo_to_gcs import MongoToGCSTemplate
from dataproc_templates.util import get_template_name, get_log_level, track_template_invocation
from dataproc_templates.gcs.gcs_to_bigquery import GCSToBigQueryTemplate
from dataproc_templates.gcs.gcs_to_gcs import GCSToGCSTemplate
from dataproc_templates.gcs.gcs_to_mongo import GCSToMONGOTemplate
from dataproc_templates.gcs.gcs_to_bigtable import GCSToBigTableTemplate
from dataproc_templates.bigquery.bigquery_to_gcs import BigQueryToGCSTemplate
from dataproc_templates.hive.hive_to_bigquery import HiveToBigQueryTemplate
from dataproc_templates.hive.hive_to_gcs import HiveToGCSTemplate
from dataproc_templates.gcs.text_to_bigquery import TextToBigQueryTemplate
from dataproc_templates.hbase.hbase_to_gcs import HbaseToGCSTemplate
from dataproc_templates.jdbc.jdbc_to_jdbc import JDBCToJDBCTemplate
from dataproc_templates.jdbc.jdbc_to_gcs import JDBCToGCSTemplate
from dataproc_templates.jdbc.jdbc_to_bigquery import JDBCToBigQueryTemplate
from dataproc_templates.snowflake.snowflake_to_gcs import SnowflakeToGCSTemplate
from dataproc_templates.redshift.redshift_to_gcs import RedshiftToGCSTemplate
from dataproc_templates.cassandra.cassandra_to_bigquery import CassandraToBQTemplate


LOGGER: logging.Logger = logging.getLogger('dataproc_templates')

# Maps each TemplateName to its corresponding implementation
# of BaseTemplate
TEMPLATE_IMPLS: Dict[TemplateName, Type[BaseTemplate]] = {
    TemplateName.GCSTOBIGQUERY: GCSToBigQueryTemplate,
    TemplateName.GCSTOGCS: GCSToGCSTemplate,
    TemplateName.GCSTOBIGTABLE: GCSToBigTableTemplate,
    TemplateName.BIGQUERYTOGCS: BigQueryToGCSTemplate,
    TemplateName.HIVETOBIGQUERY: HiveToBigQueryTemplate,
    TemplateName.HIVETOGCS: HiveToGCSTemplate,
    TemplateName.TEXTTOBIGQUERY: TextToBigQueryTemplate,
    TemplateName.GCSTOJDBC: GCSToJDBCTemplate,
    TemplateName.GCSTOMONGO: GCSToMONGOTemplate,
    TemplateName.HBASETOGCS: HbaseToGCSTemplate,
    TemplateName.JDBCTOJDBC: JDBCToJDBCTemplate,
    TemplateName.JDBCTOGCS: JDBCToGCSTemplate,
    TemplateName.JDBCTOBIGQUERY: JDBCToBigQueryTemplate,
    TemplateName.MONGOTOGCS: MongoToGCSTemplate,
    TemplateName.SNOWFLAKETOGCS: SnowflakeToGCSTemplate,
    TemplateName.REDSHIFTTOGCS: RedshiftToGCSTemplate,
    TemplateName.CASSANDRATOBQ: CassandraToBQTemplate

}

def create_spark_session(template_name: TemplateName) -> SparkSession:
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
        .appName(template_name.value) \
        .enableHiveSupport() \
        .getOrCreate()

    log4j = spark.sparkContext._jvm.org.apache.log4j
    log4j_level: log4j.Level = log4j.Level.toLevel(get_log_level())

    log4j.LogManager.getRootLogger().setLevel(log4j_level)
    log4j.LogManager.getLogger("org.apache.spark").setLevel(log4j_level)
    spark.sparkContext.setLogLevel(get_log_level())

    return spark

def run_template(template_name: TemplateName) -> None:
    """
    Executes a template given it's template name.

    Args:
        template_name (TemplateName): The TemplateName of the template
            that should be run.

    Returns:
        None
    """

    # pylint: disable=broad-except

    template_impl: Type[BaseTemplate] = TEMPLATE_IMPLS[template_name]

    template_instance: BaseTemplate = template_impl.build()

    try:
        args: Dict[str, Any] = template_instance.parse_args()

        spark: SparkSession = create_spark_session(template_name=template_name)

        track_template_invocation(spark=spark, template_name=template_name)

        template_instance.run(spark=spark, args=args)
    except Exception:
        LOGGER.exception(
            'An error occurred while running %s template',
            template_name
        )
        sys.exit(1)


if __name__ == '__main__':

    run_template(
        template_name=get_template_name()
    )
