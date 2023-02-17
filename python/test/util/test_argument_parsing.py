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

import argparse
from typing import List

import pytest

from dataproc_templates import TemplateName
from dataproc_templates.util.argument_parsing import (
    add_spark_options, get_template_name, get_log_level, spark_options_from_template_args
)
import dataproc_templates.util.template_constants as constants


def test_get_valid_template_names():
    """Tests valid template names"""
    template_names: List[str] = ["GCSTOBIGQUERY", "BIGQUERYTOGCS", "TEXTTOBIGQUERY"]

    for template_name in template_names:
        parsed_template_name: TemplateName = get_template_name(
            args=["--template",  template_name]
        )
        assert template_name == parsed_template_name.value


def test_get_invalid_template_name():
    """Tests that an invalid template name raises an error"""
    template_name = "GCSTOSMALLQUERY"
    with pytest.raises(SystemExit):
        get_template_name(["--template=" + template_name])


def test_get_valid_log_level():
    """Tests valid log levels"""
    log_levels: List[str] = ["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"]

    for log_level in log_levels:
        parsed_log_level: str = get_log_level(
            args=["--log_level",  log_level]
        )
        assert log_level == parsed_log_level


def test_get_invalid_log_level():
    """Tests that an invalid log level raises an error"""
    log_level = "AWESOME_LOG_LEVEL"
    with pytest.raises(SystemExit):
        get_template_name(["--log_level=" + log_level])


def test_add_spark_options():
    for option_set, set_name, option_prefix in [
        (constants.GCS_JDBC_INPUT_SPARK_OPTIONS, 'GCS_JDBC_INPUT_SPARK_OPTIONS', 'gcs.jdbc'),
        (constants.GCS_BQ_INPUT_SPARK_OPTIONS, 'GCS_BQ_INPUT_SPARK_OPTIONS', 'gcs.bigquery'),
        (constants.GCS_BT_INPUT_SPARK_OPTIONS, 'GCS_BT_INPUT_SPARK_OPTIONS', 'gcs.bigtable'),
        (constants.GCS_MONGO_INPUT_SPARK_OPTIONS, 'GCS_MONGO_INPUT_SPARK_OPTIONS', 'gcs.mongo'),
        (constants.GCS_TO_GCS_INPUT_SPARK_OPTIONS, 'GCS_TO_GCS_INPUT_SPARK_OPTIONS', 'gcs.to.gcs'),
        (constants.TEXT_BQ_INPUT_SPARK_OPTIONS, 'TEXT_BQ_INPUT_SPARK_OPTIONS', 'text.bigquery'),
    ]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()
        add_spark_options(parser, option_set)
        known_args, _ = parser.parse_known_args()
        for k in option_set:
            assert k.startswith(option_prefix), f'Attribute {k} does not start with {option_prefix}'
            assert hasattr(known_args, k), f'Attribute {k} missing from {set_name} args'


def test_spark_options_from_template_args():
    input_args = {
        'template.arg1': 1,
        'template.arg2': 2,
        'template.arg3': 3,
        'template.arg4': None,
    }
    option_map = {
        'template.arg1': 'real.arg1',
        'template.arg2': 'real.arg2',
        'template.arg3': 'real.arg3',
    }
    assert spark_options_from_template_args(
            input_args,
            {'template.arg2': 'real.arg2'}
        ) == {'real.arg2': 2}
    assert spark_options_from_template_args(
            input_args,
            {'template.arg1': 'real.arg1',
             'template.arg3': 'real.arg3',
             'template.arg4': 'real.arg4'}
        ) == {'real.arg1': 1,
              'real.arg3': 3}
    assert spark_options_from_template_args({}, option_map) == {}
