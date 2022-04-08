# Dataproc Templates

### Submit PySpark jobs

Submit using bin/start.sh
```
# Environment variables
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gs://path>
export SUBNET=<subnet>

# Optional environment variables
export JARS="gs://additional/dependency.jar"
export HISTORY_SERVER_CLUSTER=projects/{projectId}/regions/{regionId}/clusters/{clusterId}
export METASTORE_SERVICE=projects/{projectId}/locations/{regionId}/services/{serviceId}

./bin/start.sh -- --template=TEMPLATENAME --my.property="value" --my.other.property="value" (etc...)
```

Submit using gcloud CLI
```
python setup.py bdist_egg
export PACKAGE_EGG_FILE=dist/dataproc_templates-0.0.1-py3.8.egg

gcloud dataproc batches submit pyspark \
      --region=<region> \
      --project=<project_id> \
      --jars="<required_jar_dependencies>" \
      --deps-bucket=<gs://path> \
      --subnet=<subnet> \
      --py-files=${PACKAGE_EGG_FILE} \
      main.py \
      -- --<my.property>=<value> \
         --<my.other.property>=<value>
```

See each template's README to know the specific required arguments

- [GCS](dataproc_templates/gcs/README.md)
  - GCSTOBIGQUERY
- [BIGQUERY](dataproc_templates/bigquery/README.md)
  - BIGQUERYTOGCS