# Dataproc Templates

### Submit PySpark jobs

Submit using bin/start.sh
```
export GCP_PROJECT=<project>
export REGION=<region>
export GCS_STAGING_LOCATION=<gs://path>
export SUBNET=<subnet>

./bin/start.sh [template_folder/template_name] -- [--key=value]

### example: ./bin/start.sh gcs/gcs_to_bigquery -- --<optional.application.argument>=<arg_value>
```

Submit using gcloud CLI
```
gcloud dataproc batches submit pyspark \
      --region=<region> \
      --project=<project> \
      --jars="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar" \
      --deps-bucket=<gs://path> \
      --subnet=<subnet> \
      --py-files="src/templates/util/template_constants.py" \
      --files="src/templates/resources/default_args.ini" \
      src/templates/<template_folder>/<template_name>.py \
      -- --<optional.application.argument>=<arg_value>
```