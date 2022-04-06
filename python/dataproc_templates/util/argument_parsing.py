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

import argparse


def get_template_name() -> str:
    """
    Parses the template name option from the command line.

    Returns:
        str: The value of the --template argument
    """

    parser: argparse.ArgumentParser = argparse.ArgumentParser()

    parser.add_argument(
        '--template',
        dest='template_name',
        type=str,
        help='The name of the template to run',
        required=True
    )

    known_args: argparse.Namespace
    known_args, _ = parser.parse_known_args()

    return known_args.template_name
