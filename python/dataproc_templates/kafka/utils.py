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

import logging
from typing import List

from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import RegisteredSchema
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry import SchemaReference
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.functions import udf

logger = logging.getLogger(__name__)


def get_kafka_admin_client(bootstrap_servers: str = None,
    api_key: str = None, api_secret: str = None):
    try:
        _conf = {
            "bootstrap.servers": bootstrap_servers
        }
        if api_key:
            _conf["security.protocol"] = "SASL_SSL"
            _conf["sasl.mechanisms"] = "PLAIN"
            _conf["sasl.username"] = api_key,
            _conf["sasl.password"] = api_secret

        return AdminClient(conf=_conf)
    except Exception as e:
        logger.exception(
            f"Error creating AdminClient using bootstrap_servers={bootstrap_servers}, api_key={api_key} and api_secret={api_secret}")
        raise e


def get_schema_registry_client(endpoint: str = None,
    api_key: str = None, api_secret: str = None):
    try:
        _conf = {
            "url": endpoint
        }
        if api_key:
            _conf["basic.auth.user.info"] = f"{api_key}:{api_secret}"

        return SchemaRegistryClient(_conf)
    except Exception as e:
        logger.exception(
            f"Error creating SchemaRegistryClient using endpoint={endpoint}, api_key={api_key} and api_secret={api_secret}")
        raise e


@udf
def gslv(subject: str = None,
    endpoint: str = None,
    api_key: str = None,
    api_secret: str = None):
    """

    :param subject:
    :param endpoint:
    :param api_key:
    :param api_secret:
    :return:
    """
    res = None
    try:
        schema_registry = get_schema_registry_client(endpoint, api_key, api_secret)

        lv: RegisteredSchema = schema_registry.get_latest_version(subject)
        schema: Schema = lv.schema
        references: List[SchemaReference] = schema.references
        res = {
            "schema_id": lv.schema_id,
            "schema": {
                "schema_str": schema.schema_str,
                "schema_type": schema.schema_type,
                "references": [
                    {
                        "name": r.name,
                        "subject": r.subject,
                        "version": r.version,
                    }
                    for r in references
                ]
            },
            "subject": lv.subject,
            "version": lv.version
        }
        return str(res)
    except Exception as e:
        logging.exception(
            f"Error getting schema latest version for {subject}")
        raise e
