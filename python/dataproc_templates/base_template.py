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

from __future__ import annotations

from typing import Dict, Sequence, Optional, Any
from abc import ABC as AbstractClass, abstractmethod
from logging import Logger

from pyspark.sql import SparkSession

__all__ = ['BaseTemplate']


class BaseTemplate(AbstractClass):
    """Base class for all Dataproc Templates"""

    def get_logger(self, spark: SparkSession) -> Logger:
        """
        Convenience method to get the Spark logger from a SparkSession

        Args:
            spark (SparkSession): The initialized SparkSession object

        Returns:
            Logger: The Spark logger
        """

        log_4j_logger = spark.sparkContext._jvm.org.apache.log4j  # pylint: disable=protected-access
        return log_4j_logger.LogManager.getLogger(__name__)

    @classmethod
    def build(cls) -> BaseTemplate:
        """
        Factory method for building an instance of this template class.
        """

        return cls()

    @staticmethod
    @abstractmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        """
        Parses this template's arguments, returning them as a dictionary.

        Implementations of this method should ignore unknown arguments.

        Args:
            args (Optional[Sequence[str]]): The template arguments.
                By default, command line arguments are used.
        """

    @abstractmethod
    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:
        """
        Runs this template
        """
