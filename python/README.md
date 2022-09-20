![Build Status](https://dataproctemplatesci.com/buildStatus/icon?job=dataproc-templates-build%2Fbuild-job-python&&subject=python-build)

# Dataproc Templates (Python - PySpark)

* [BigQueryToGCS](/python/dataproc_templates/bigquery/README.md) (blogpost [link](https://medium.com/google-cloud/moving-data-from-bigquery-to-gcs-using-gcp-dataproc-serverless-and-pyspark-f6481b86bcd1))
* [GCSToBigQuery](/python/dataproc_templates/gcs/README.md) (blogpost [link](https://medium.com/@ppaglilla/getting-started-with-dataproc-serverless-pyspark-templates-e32278a6a06e))
* [GCSToBigTable](/python/dataproc_templates/gcs/README.md)
* [GCSToJDBC](/python/dataproc_templates/gcs/README.md)
* [GCSToMongo](/python/dataproc_templates/gcs/README.md) (blogpost [link](https://medium.com/google-cloud/importing-data-from-gcs-to-mongodb-using-dataproc-serverless-fed58904633a))
* [HiveToBigQuery](/python/dataproc_templates/hive/README.md) (blogpost [link](https://medium.com/google-cloud/processing-data-from-hive-to-bigquery-using-pyspark-and-dataproc-serverless-217c7cb9e4f8))
* [HiveToGCS](/python/dataproc_templates/hive/README.md)(blogpost [link](https://medium.com/@surjitsh/processing-large-data-tables-from-hive-to-gcs-using-pyspark-and-dataproc-serverless-35d3d16daaf))
* [HbaseToGCS](/python/dataproc_templates/hbase/README.md)
* [MongoToGCS](/python/dataproc_templates/mongo/README.md)
* [JDBCToJDBC](/python/dataproc_templates/jdbc/README.md)
* [JDBCToGCS](/python/dataproc_templates/jdbc/README.md)
* [RedshiftToGCS](/python/dataproc_templates/redshift/README.md)
* [TextToBigQuery](/python/dataproc_templates/gcs/README.md)
* [GCSToGCS](/python/dataproc_templates/gcs/README.md)
* [SnowflakeoGCS](/python/dataproc_templates/snowflake/README.md)

Dataproc Templates (Python - PySpark) submit jobs to Dataproc Serverless using [batches submit pyspark](https://cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit/pyspark).

## Setting up the local environment

It is recommended to use a [virtual environment](https://docs.python.org/3/library/venv.html) when setting up the local environment. This setup is not required for submitting templates, only for running and developing locally.

``` bash
# Create a virtual environment, activate it and install requirements
mkdir venv
python -m venv venv/
source venv/bin/activate
pip install -r requirements.txt
```

## Running unit tests

Unit tests are developed using [`pytest`](https://docs.pytest.org/en/7.1.x/).

To run all unit tests, simply run pytest:

``` bash
pytest
```

To generate a coverage report, run the tests using coverage

``` bash
coverage run \
  --source=dataproc_templates \
  --module pytest \
  --verbose \
  test

coverage report --show-missing
```

## Submitting templates to Dataproc Serverless

A shell script is provided to:
- Build the python package
- Set Dataproc parameters based on environment variables
- Submit the desired template to Dataproc with the provided template parameters
 
**bin/start.sh usage**:

```
# Set required environment variables
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gs://path>

# Set optional environment variables
export SUBNET=<subnet>
export JARS="gs://additional/dependency.jar"
export HISTORY_SERVER_CLUSTER=projects/{projectId}/regions/{regionId}/clusters/{clusterId}
export METASTORE_SERVICE=projects/{projectId}/locations/{regionId}/services/{serviceId}

# Submit to Dataproc passing template parameters
./bin/start.sh [--properties=<spark.something.key>=<value>] \
               -- --template=TEMPLATENAME \
                  --my.property="<value>" \
                  --my.other.property="<value>"
                  (etc...)
```

**gcloud CLI usage**:

It is also possible to submit jobs using the `gcloud` CLI directly. That can be achieved by:

1. Building the `dataproc_templates` package into an `.egg`

``` bash
PACKAGE_EGG_FILE=dist/dataproc_templates_distribution.egg
python setup.py bdist_egg --output=${PACKAGE_EGG_FILE}
```

2. Submitting the job
  * The `main.py` file should be the main python script
  * The `.egg` file for the package must be bundled using the `--py-files` flag

```
gcloud dataproc batches submit pyspark \
      --region=<region> \
      --project=<project_id> \
      --jars="<required_jar_dependencies>" \
      --deps-bucket=<gs://path> \
      --subnet=<subnet> \
      --py-files=${PACKAGE_EGG_FILE} \
      [--properties=<spark.something.key>=<value>] \
      main.py \
      -- --template=TEMPLATENAME \
         --<my.property>="<value>" \
         --<my.other.property>="<value>"
         (etc...)
```

To see each template's specific parameters, refer to each template's README.

Refer to this [documentation](https://cloud.google.com/dataproc-serverless/docs/concepts/properties) to see the available spark properties.


**Vertex AI usage**:

Follow [Dataproc Templates (Jupyter Notebooks) README](./notebooks/README.md) to submit Dataproc Templates from a Vertex AI notebook.  
