"""
Setup script for the dataproc_templates package

It extends the bdist_egg command to allow renaming of the output .egg artifact.
Usage: `python setup.py bdist_egg --output=my-custom-artifact-name.egg`
"""

from typing import Optional
import shutil

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

    user_options = [('output=', 'o', 'output file name')] + bdist_egg.user_options

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


setup(
    name="dataproc-templates",
    version="0.0.1",
    description="Dataproc templates written in Python",
    url="https://github.com/GoogleCloudPlatform/dataproc-templates",
    packages=find_packages(exclude=['test']),
    install_requires=[
        'pyspark>=3.2.0'
    ],
    # Override the bdist_egg command with our extension
    cmdclass={
        'bdist_egg': BdistEggCustomEggName
    }
)
