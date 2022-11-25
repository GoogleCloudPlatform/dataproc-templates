"""
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""

import pytest
from pyspark.sql import SparkSession

from dataproc_templates import TemplateName
from dataproc_templates.kafka.kafkametadata_to_gcs import \
    KafkaMetadataToGCSTemplate


class TestKafkaMetadataToGCSTemplate:
    """
    Test suite for KafkaMetadataToGCSTemplate
    """

    @pytest.fixture
    def spark_session(self) -> SparkSession:
        """
        Creates the SparkSession object.

        It also sets the Spark logging level to info. We could
        consider parametrizing the log level in the future.

        Returns:
            pyspark.sql.SparkSession: The set up SparkSession.
        """

        spark = SparkSession.builder \
            .appName(TemplateName.KAFKAMETADATATOGCS.value) \
            .enableHiveSupport() \
            .getOrCreate()

        log4j = spark.sparkContext._jvm.org.apache.log4j
        log4j_level: log4j.Level = log4j.Level.toLevel("DEBUG")

        log4j.LogManager.getRootLogger().setLevel(log4j_level)
        log4j.LogManager.getLogger("org.apache.spark").setLevel(log4j_level)
        spark.sparkContext.setLogLevel("DEBUG")

        return spark

    def test_parse_all_args(self):
        """Tests KafkaMetadataToGCSTemplate.parse_args()"""

        kafkametadata_to_gcs_template = KafkaMetadataToGCSTemplate()
        parsed_args = kafkametadata_to_gcs_template.parse_args(
            [
                "--kafkametadata.gcs.bootstrap.servers=kafka:9092",
                "--kafkametadata.gcs.api.key=api_key",
                "--kafkametadata.gcs.api.secret=api_secret",
                "--kafkametadata.gcs.schemaregistry.endpoint=http://schema_registry",
                "--kafkametadata.gcs.schemaregistry.api.key=sr_api_key",
                "--kafkametadata.gcs.schemaregistry.api.secret=sr_api_secret",
                "--kafkametadata.gcs.output.location=gs://test",
                "--kafkametadata.gcs.output.format=parquet",
                "--kafkametadata.gcs.output.mode=overwrite"
            ])

        assert parsed_args[
                   "kafkametadata.gcs.bootstrap.servers"] == "kafka:9092"
        assert parsed_args["kafkametadata.gcs.api.key"] == "api_key"
        assert parsed_args["kafkametadata.gcs.api.secret"] == "api_secret"
        assert parsed_args[
                   "kafkametadata.gcs.schemaregistry.endpoint"] == "http://schema_registry"
        assert parsed_args[
                   "kafkametadata.gcs.schemaregistry.api.key"] == "sr_api_key"
        assert parsed_args[
                   "kafkametadata.gcs.schemaregistry.api.secret"] == "sr_api_secret"
        assert parsed_args["kafkametadata.gcs.output.location"] == "gs://test"
        assert parsed_args["kafkametadata.gcs.output.format"] == "parquet"
        assert parsed_args["kafkametadata.gcs.output.mode"] == "overwrite"

    def test_parse_required_args(self):
        """Tests KafkaMetadataToGCSTemplate.parse_args()"""

        kafkametadata_to_gcs_template = KafkaMetadataToGCSTemplate()
        parsed_args = kafkametadata_to_gcs_template.parse_args(
            [
                "--kafkametadata.gcs.bootstrap.servers=kafka:9092",
                "--kafkametadata.gcs.schemaregistry.endpoint=http://schema_registry",
                "--kafkametadata.gcs.output.location=gs://test"
            ])

        assert parsed_args[
                   "kafkametadata.gcs.bootstrap.servers"] == "kafka:9092"
        assert parsed_args["kafkametadata.gcs.api.key"] == None
        assert parsed_args["kafkametadata.gcs.api.secret"] == None
        assert parsed_args[
                   "kafkametadata.gcs.schemaregistry.endpoint"] == "http://schema_registry"
        assert parsed_args["kafkametadata.gcs.schemaregistry.api.key"] == None
        assert parsed_args[
                   "kafkametadata.gcs.schemaregistry.api.secret"] == None
        assert parsed_args["kafkametadata.gcs.output.location"] == "gs://test"
        assert parsed_args["kafkametadata.gcs.output.format"] == "parquet"
        assert parsed_args["kafkametadata.gcs.output.mode"] == "overwrite"

    def test_run_localhost_no_credentials(self, spark_session):
        """Tests KafkaMetadataToGCSTemplate write parquet"""

        kafkametadata_to_gcs_template = KafkaMetadataToGCSTemplate()

        mock_parsed_args = kafkametadata_to_gcs_template.parse_args(
            [
                "--kafkametadata.gcs.bootstrap.servers=localhost:9092",
                "--kafkametadata.gcs.schemaregistry.endpoint=http://localhost:8081",
                "--kafkametadata.gcs.output.location=gs://test/kafkametadatatogsc/",
                "--kafkametadata.gcs.output.format=parquet",
                "--kafkametadata.gcs.output.mode=overwrite"
            ])
        kafkametadata_to_gcs_template.run(spark_session, mock_parsed_args)
        assert 1 == 1
