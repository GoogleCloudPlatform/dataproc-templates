import setuptools

setuptools.setup(
    name="dataproc-templates",
    version="0.0.1",
    description="Dataproc templates written in Python",
    packages=setuptools.find_packages(exclude=['test']),
    install_requires=[
        'pyspark>=3.2.0'
    ]
)
