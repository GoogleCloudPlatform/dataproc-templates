![Build Status](https://dataproctemplatesci.com/buildStatus/icon?job=dataproc-templates-build%2Fbuild-job-python&&subject=python-build)

# Dataproc Templates (Python - PySpark)
* [BigQueryToGCS](/python/dataproc_templates/bigquery#bigquery-to-gcs) (blogpost [link](https://medium.com/google-cloud/moving-data-from-bigquery-to-gcs-using-gcp-dataproc-serverless-and-pyspark-f6481b86bcd1))
* [CassandraToBigquery](/python/dataproc_templates/cassandra/README.md)
* [GCSToBigQuery](/python/dataproc_templates/gcs#gcs-to-bigquery) (blogpost [link](https://medium.com/@ppaglilla/getting-started-with-dataproc-serverless-pyspark-templates-e32278a6a06e))
* [GCSToBigTable](/python/dataproc_templates/gcs#gcs-to-bigtable) (blogpost [link](https://medium.com/google-cloud/pyspark-load-data-from-gcs-to-bigtable-using-gcp-dataproc-serverless-c373430fe157))
* [GCSToGCS](/python/dataproc_templates/gcs#gcs-to-gcs---sql-transformation)(blogpost [link](https://medium.com/@ankuljain/migrate-gcs-to-gcs-using-dataproc-serverless-3b7b0f6ad6b9))
* [GCSToJDBC](/python/dataproc_templates/gcs#gcs-to-jdbc) (blogpost [link](https://medium.com/google-cloud/import-data-from-gcs-to-jdbc-databases-using-dataproc-serverless-c7154b242430))
* [GCSToMongo](/python/dataproc_templates/gcs#gcs-to-mongodb) (blogpost [link](https://medium.com/google-cloud/importing-data-from-gcs-to-mongodb-using-dataproc-serverless-fed58904633a))
* [HbaseToGCS](/python/dataproc_templates/hbase#hbase-to-gcs)
* [HiveToBigQuery](/python/dataproc_templates/hive#hive-to-bigquery) (blogpost [link](https://medium.com/google-cloud/processing-data-from-hive-to-bigquery-using-pyspark-and-dataproc-serverless-217c7cb9e4f8))
* [HiveToGCS](/python/dataproc_templates/hive#hive-to-gcs)(blogpost [link](https://medium.com/@surjitsh/processing-large-data-tables-from-hive-to-gcs-using-pyspark-and-dataproc-serverless-35d3d16daaf))
* [JDBCToBigQuery](/python/dataproc_templates/jdbc#3-jdbc-to-bigquery) (blogpost [link](https://medium.com/@sjlva/python-fast-export-large-database-tables-using-gcp-serverless-dataproc-bfe77a132485))
* [JDBCToGCS](/python/dataproc_templates/jdbc#2-jdbc-to-gcs) (blogpost [link](https://medium.com/google-cloud/importing-data-from-databases-into-gcs-via-jdbc-using-dataproc-serverless-f330cb0160f0))
* [JDBCToJDBC](/python/dataproc_templates/jdbc#1-jdbc-to-jdbc) (blogpost [link](https://medium.com/google-cloud/migrating-data-from-one-databases-into-another-via-jdbc-using-dataproc-serverless-c5336c409b18))
* [MongoToGCS](/python/dataproc_templates/mongo#mongo-to-gcs)(blogpost [link](https://medium.com/google-cloud/exporting-data-from-mongodb-to-gcs-buckets-using-dataproc-serverless-64830fb15b51))
* [RedshiftToGCS](/python/dataproc_templates/redshift#redshift-to-gcs)(blogpost [link](https://medium.com/google-cloud/exporting-data-from-redshift-to-gcs-using-gcp-dataproc-serverless-and-pyspark-9ab78de11405))
* [SnowflakeToGCS](/python/dataproc_templates/snowflake#1-snowflake-to-gcs)(blogpost [link](https://medium.com/@varunikagupta96/exporting-data-from-snowflake-to-gcs-using-pyspark-on-dataproc-serverless-363d3bed551b))
* [TextToBigQuery](/python/dataproc_templates/gcs#text-to-bigquery)

Dataproc Templates (Python - PySpark) submit jobs to Dataproc Serverless using [batches submit pyspark](https://cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit/pyspark).

## Run using PyPi package

In this README, you see instructions on how to submit Dataproc Serverless template jobs.  
Currently, 3 options are described:
- Using bin/start.sh
- Using gcloud CLI
- Using Vertex AI

Those 3 options require you to clone this repo and start running the templates.  
The [Dataproc Templates PyPi package](https://pypi.org/project/google-dataproc-templates) is a **4th option** to run templates from a PySpark environment directly (Dataproc or local/another).  
Example:  

```
!pip3 install --user google-dataproc-templates==0.0.3

from dataproc_templates.bigquery.bigquery_to_gcs import BigQueryToGCSTemplate
from pyspark.sql import SparkSession

args = dict()
args["bigquery.gcs.input.table"] = "<bq_dataset>.<bq_table>"
args["bigquery.gcs.input.location"] = "<location>"
args["bigquery.gcs.output.format"] = "<format>"
args["bigquery.gcs.output.mode"] = "<mode>"
args["bigquery.gcs.output.location"] = "gs://<bucket_name/path>"

spark = SparkSession.builder \
        .appName("BIGQUERYTOGCS") \
        .enableHiveSupport() \
        .getOrCreate()

template = BigQueryToGCSTemplate()
template.run(spark, args)
```

**Pro Tip**: [Start a Dataproc Serverless Spark sessions](https://cloud.google.com/vertex-ai/docs/workbench/managed/serverless-spark#start_a_spark_session) in a Vertex AI managed notebook, and leverage a serverless Spark session, in which your job will run using Dataproc Serverless, instead of your local PySpark environment.

While this provides an easy way to get started, remember that the bin/start.sh already provides an easy way for you to, for example, specify required .jar dependencies. Using the PyPi package, you need to configure your PySpark sessions in accordance with the requirements of your specific template. You would need to, for example, specify the spark.driver.extraClassPath configuration:

```
spark = SparkSession.builder \
        ... \
        .config('spark.driver.extraClassPath', '<template_required_dependency>.jar')
        ... \
        .getOrCreate()
```

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

<hr>

When submitting, there are 3 types of properties/parameters for the user to provide.  
- **Spark properties**: Refer to this [documentation](https://cloud.google.com/dataproc-serverless/docs/concepts/properties) to see the available spark properties.
- **Each template's specific parameters**: refer to each template's README.
- **Common arguments**: --template_name and --log_level
  - The **--log_level** parameter is optional, it defaults to INFO.
    - Possible choices are the Spark log levels: ["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"].





<hr>

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
                  --log_level=INFO \
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
         --log_level=INFO \
         --<my.property>="<value>" \
         --<my.other.property>="<value>"
         (etc...)
```

**Vertex AI usage**:

Follow [Dataproc Templates (Jupyter Notebooks) README](../notebooks/README.md) to submit Dataproc Templates from a Vertex AI notebook.
