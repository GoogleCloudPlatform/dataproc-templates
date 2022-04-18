# Dataproc Templates (Python - PySpark)

* [GCSToBigQuery](dataproc_templates/gcs/README.md)
* [BigQueryToGCS](dataproc_templates/bigquery/README.md)

Dataproc Templates (Python - PySpark) submit jobs to Dataproc Serverless using [batches submit pyspark ](https://cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit/pyspark).

## Requirements

- Python 3.8

## Run unit tests
```
coverage run --source=dataproc_templates --module pytest --verbose test && coverage report --show-missing
```

## Submit templates to Dataproc Serverless

A shell script is provided to:
 - Build the python package
 - Set Dataproc parameters based on environment variables
 - Submit the desired template to Dataproc with the provided template parameters
 
**bin/start.sh usage syntax**:
```
# Set required environment variables
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gs://path>
export SUBNET=<subnet>

# Set optional environment variables
export JARS="gs://additional/dependency.jar"
export HISTORY_SERVER_CLUSTER=projects/{projectId}/regions/{regionId}/clusters/{clusterId}
export METASTORE_SERVICE=projects/{projectId}/locations/{regionId}/services/{serviceId}

# Submit to Dataproc passing template parameters
./bin/start.sh -- --template=TEMPLATENAME \
                  --my.property="<value>" \
                  --my.other.property="<value>"
                  (etc...)
```
To see template's specific parameters, refer to each template's README.

It is also possible to submit the jobs using gcloud CLI, after building the package.

**gcloud CLI usage syntax**:
```
# Build the package
python setup.py bdist_egg
export PACKAGE_EGG_FILE=dist/dataproc_templates-0.0.1-py3.8.egg

# Submit passing Dataproc and template parameters
gcloud dataproc batches submit pyspark \
      --region=<region> \
      --project=<project_id> \
      --jars="<required_jar_dependencies>" \
      --deps-bucket=<gs://path> \
      --subnet=<subnet> \
      --py-files=${PACKAGE_EGG_FILE} \
      main.py \
      -- --template=TEMPLATENAME \
         --<my.property>="<value>" \
         --<my.other.property>="<value>"
         (etc...)
```
