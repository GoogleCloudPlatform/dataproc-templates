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

from typing import List
from enum import Enum

__all__ = ["ScriptName"]


class ScriptName(Enum):
    """Enumeration of all script types"""

    HIVETOBIGQUERY = "HIVETOBIGQUERY"
    MYSQLTOSPANNER = "MYSQLTOSPANNER"
    ORACLETOPOSTGRES = "ORACLETOPOSTGRES"
    ORACLETOBIGQUERY = "ORACLETOBIGQUERY"
    POSTGRESTOBIGQUERY = "POSTGRESTOBIGQUERY"

    @classmethod
    def from_string(cls, script_name: str) -> ScriptName:
        """
        Get the ScriptName value from a string.

        This is not case-sensitive.

        Args:
            script_name (str): The string representation of
                the script name.

        Returns:
            ScriptName: the ScriptName value corresponding
                to the provided string

        Raises:
            ValueError: if the given script name is invalid
        """

        try:
            return cls[script_name.upper()]
        except KeyError as err:
            raise ValueError(f"Invalid script name {script_name}") from err

    @classmethod
    def choices(cls) -> List[str]:
        """
        Returns all the available ScriptName options as strings

        Returns:
            List[str]: All available ScriptName options
        """

        return [script_name.value for script_name in cls]
