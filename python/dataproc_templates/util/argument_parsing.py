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

from typing import Optional, Sequence
import argparse

from dataproc_templates import TemplateName


def get_template_name(args: Optional[Sequence[str]] = None) -> TemplateName:
    """
    Parses the template name option from the program arguments.

    This will print the command help and exit if the --template
    argument is missing.

    Args:
        args (Optional[Sequence[str]]): The program arguments.
            By default, command line arguments will be used.

    Returns:
        str: The value of the --template argument
    """

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        add_help=False
    )

    parser.add_argument(
        '--template',
        dest='template_name',
        type=str,
        required=False,
        default=None,
        choices=TemplateName.choices(),
        help='The name of the template to run'
    )

    known_args: argparse.Namespace
    known_args, _ = parser.parse_known_args(args=args)

    if known_args.template_name is None:
        parser.print_help()
        parser.exit()

    return TemplateName.from_string(known_args.template_name)
