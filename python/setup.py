# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Setup script for the dataproc_templates package

It extends the bdist_egg command to allow renaming of the output .egg artifact.
Usage: `python setup.py bdist_egg --output=my-custom-artifact-name.egg`
"""

from typing import Optional
import shutil
import io
import os
from distutils import log
from setuptools import setup, find_packages
from setuptools.command.bdist_egg import bdist_egg


class BdistEggCustomEggName(bdist_egg):
    """
    Create an "egg" distrubution, optionally renaming the output .egg file

    This custom command extends the bdist_egg command. It adds the
    `--output` argument for specifying the name of the output .egg file.
    If it's provided, the egg artifact is renamed after it's built.
    """

    description: str = 'create an "egg" distrubution, optionally renaming the output .egg file'

    user_options = [('output=', 'o', 'output file name')] + \
        bdist_egg.user_options

    def __init__(self, *args, **kwargs):
        self.output: Optional[str] = None
        super().__init__(*args, **kwargs)

    def initialize_options(self):
        """Initialize command options"""
        super().initialize_options()
        self.output = None

    def run(self):
        """
        Run the command

        First execute `bdist_egg` as usual. Then, rename the .egg file
        if the `--output` argument was specified.
        """

        super().run()

        built_egg_path: str = self.get_outputs()[0]

        if self.output is not None:
            log.info(
                'Will rename output .egg file from %s to %s',
                built_egg_path,
                self.output
            )
            shutil.move(src=built_egg_path, dst=self.output)


dependencies = [
    "pyspark>=3.2.0",
    "google-cloud-bigquery>=3.4.0"
]


package_root = os.path.abspath(os.path.dirname(__file__))

version = {}
with open(
    os.path.join(package_root, "version.py")
) as fp:
    exec(fp.read(), version)
version = version["__version__"]

readme_filename = os.path.join(package_root, "README.md")
with io.open(readme_filename, encoding="utf-8") as readme_file:
    readme = readme_file.read()

setup(
    name="google-dataproc-templates",
    version=version,
    description="Google Dataproc templates written in Python",
    long_description_content_type="text/markdown",
    long_description=readme,
    license="Apache 2.0",
    url="https://github.com/GoogleCloudPlatform/dataproc-templates",
    packages=find_packages(exclude=['test']),
    install_requires=dependencies,
    # Override the bdist_egg command with our extension
    cmdclass={
        'bdist_egg': BdistEggCustomEggName
    },
    platforms="Posix; MacOS X; Windows",
    python_requires=">=3.7"
)
