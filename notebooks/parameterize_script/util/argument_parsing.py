# Copyright 2023 Google LLC
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

from typing import Optional, Sequence
import argparse

from parameterize_script.script_name import ScriptName


def get_script_name(args: Optional[Sequence[str]] = None) -> ScriptName:
    """
    Parses the script name option from the program arguments.

    This will print the command help and exit if the --script
    argument is missing.

    Args:
        args (Optional[Sequence[str]]): The program arguments.
            By default, command line arguments will be used.

    Returns:
        str: The value of the --script argument
    """

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        add_help=False
    )

    parser.add_argument(
        '--script',
        dest='script_name',
        type=str,
        required=False,
        default=None,
        choices=ScriptName.choices(),
        help='The name of the script to run'
    )

    known_args: argparse.Namespace
    known_args, _ = parser.parse_known_args(args=args)

    if known_args.script_name is None:
        parser.print_help()
        parser.exit()

    return ScriptName.from_string(known_args.script_name)

def get_log_level(args: Optional[Sequence[str]] = None) -> str:
    """
    Parses the log level option from the program arguments.
    INFO is the default log level
    This will exit if the log level in an invalid choice.
    Args:
        args (Optional[Sequence[str]]): The program arguments.
            By default, command line arguments will be used.
    Returns:
        str: The value of the --log_level argument
    """

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        add_help=False
    )

    parser.add_argument(
        '--log_level',
        dest='log_level',
        type=str,
        required=False,
        default="INFO",
        choices=["NOTSET","DEBUG","INFO","WARNING","ERROR","CRITICAL"],
        help='Papermill Execute Notebook Log Level'
    )

    known_args: argparse.Namespace
    known_args, _ = parser.parse_known_args(args=args)

    return known_args.log_level
