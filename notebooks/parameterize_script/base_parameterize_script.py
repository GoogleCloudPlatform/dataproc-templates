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

from __future__ import annotations

from os import environ
from typing import Dict, Sequence, Optional, Any
from abc import ABC as AbstractClass, abstractmethod
import parameterize_script.util.notebook_constants as constants

__all__ = ["BaseParameterizeScript"]


class BaseParameterizeScript(AbstractClass):
    """Base class for all Parameterize scripts."""

    @classmethod
    def build(cls) -> BaseParameterizeScript:
        """
        Factory method for building an instance of this script class.
        """

        return cls()

    @staticmethod
    @abstractmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        """
        Parses this script's arguments, returning them as a dictionary.

        Implementations of this method should ignore unknown arguments.

        Args:
            args (Optional[Sequence[str]]): The script arguments.
                By default, command line arguments are used.
        """

    @staticmethod
    def get_env_vars() -> Dict[str, Any]:
        """
        Gets the environment variables.
        """
        env_vars = {}
        env_vars[constants.PROJECT] = environ[constants.GCP_PROJECT]
        env_vars[constants.REGION] = environ[constants.REGION]
        env_vars[constants.GCS_STAGING_LOCATION] = environ[
            constants.GCS_STAGING_LOCATION
        ]
        env_vars[constants.SUBNET] = (
            environ[constants.SUBNET] if constants.SUBNET in environ else ""
        )
        env_vars[constants.SERVICE_ACCOUNT] = (
            environ[constants.SERVICE_ACCOUNT]
            if constants.SERVICE_ACCOUNT in environ
            else ""
        )
        env_vars[constants.IS_PARAMETERIZED] = True

        return env_vars

    @abstractmethod
    def run(self, args: Dict[str, Any]) -> None:
        """
        Runs this script
        """
