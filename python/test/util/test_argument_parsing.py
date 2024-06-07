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

import dataproc_templates.util.template_constants as constants
from dataproc_templates.util.template_constants import \
    get_csv_input_spark_options as in_csv_opt
from dataproc_templates.util.template_constants import \
    get_csv_output_spark_options as out_csv_opt
from dataproc_templates.util.template_constants import \
get_es_spark_connector_input_options as es_spark_opt
from dataproc_templates import TemplateName
from dataproc_templates.util.argument_parsing import (
    add_spark_options,
    add_es_spark_connector_options,
    get_template_name,
    get_log_level,
)


def test_get_valid_template_names():
    """Tests valid template names"""
    template_names: List[str] = ["GCSTOBIGQUERY", "BIGQUERYTOGCS",
                                 "TEXTTOBIGQUERY"]

    for template_name in template_names:
        parsed_template_name: TemplateName = get_template_name(
            args=["--template", template_name]
        )
        assert template_name == parsed_template_name.value


def test_get_invalid_template_name():
    """Tests that an invalid template name raises an error"""
    template_name = "GCSTOSMALLQUERY"
    with pytest.raises(SystemExit):
        get_template_name(["--template=" + template_name])


def test_get_valid_log_level():
    """Tests valid log levels"""
    log_levels: List[str] = [
        "ALL",
        "DEBUG",
        "ERROR",
        "FATAL",
        "INFO",
        "OFF",
        "TRACE",
        "WARN",
    ]

    for log_level in log_levels:
        parsed_log_level: str = get_log_level(args=["--log_level", log_level])
        assert log_level == parsed_log_level


def test_get_invalid_log_level():
    """Tests that an invalid log level raises an error"""
    log_level = "AWESOME_LOG_LEVEL"
    with pytest.raises(SystemExit):
        get_template_name(["--log_level=" + log_level])


def test_add_spark_options():
    for option_set, option_prefix in [
        # Read options
        (in_csv_opt("mock.in.prefix."), "mock.in.prefix"),
        # Write options
        (out_csv_opt("mock.out.prefix."), "mock.out.prefix"),
    ]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()
        add_spark_options(parser, option_set)
        known_args, _ = parser.parse_known_args()
        for k in option_set:
            assert k.startswith(
                option_prefix
            ), f"Attribute {k} does not start with {option_prefix}"
            assert hasattr(
                known_args, k
            ), f"Attribute {k} missing from {option_prefix} args"
            assert (
                option_set[k] in constants.SPARK_OPTIONS
            ), f"Attribute {k} maps to invalid Spark option {option_set[k]}"
            spark_option = option_set[k]
            assert (
                constants.SPARK_OPTIONS[spark_option].get(
                    constants.OPTION_HELP)
                or constants.SPARK_OPTIONS[spark_option].get(
                    constants.OPTION_READ_HELP)
                or constants.SPARK_OPTIONS[spark_option].get(
                    constants.OPTION_WRITE_HELP
                )
            ), f"Attribute {spark_option} has no help text in SPARK_OPTIONS"

def test_add_es_spark_connector_options():
    """Test add_es_spark_connector_options function"""
    for option_set, option_prefix in [
        (es_spark_opt("mock.in.prefix."), "mock.in.prefix"),
    ]:
        parser: argparse.ArgumentParser = argparse.ArgumentParser()
        add_es_spark_connector_options(parser, option_set)
        known_args, _ = parser.parse_known_args()
        for k in option_set:
            assert k.startswith(
                option_prefix
            ), f"Attribute {k} does not start with {option_prefix}"
            assert hasattr(
                known_args, k
            ), f"Attribute {k} missing from {option_prefix} args"
            assert (
                option_set[k] in constants.ES_SPARK_READER_OPTIONS
            ), f"Attribute {k} maps to invalid Elasticsearch Spark option {option_set[k]}"
            es_spark_reader_option = option_set[k]
            assert (
                constants.ES_SPARK_READER_OPTIONS[es_spark_reader_option].get(
                    constants.OPTION_HELP)
            ), f"Attribute {es_spark_reader_option} has no help text in SPARK_OPTIONS"
