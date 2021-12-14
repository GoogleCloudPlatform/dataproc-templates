## General Template

The general template is configured via a yaml file.
For this file to be accessible, it must be provided on startup using the spark submit argument `--files`. 
The template must be provided the file name with the template argument `--config`.

### Start the general template using a config file uploaded to GCS:

Export environment variables:
```bash
export GCP_PROJECT=<project id>
export REGION=<region name>
export GCS_STAGING_LOCATION=<gcs path>
export SUBNET=projects/<project id>/regions/<region name>/subnetworks/<subnetwork name>
```

Copy your config file to GCS
```
gsutil cp config.yaml gs://bucket/path/config.yaml
```

Submit the template: 
```bash
bin/start_v2.sh \
--files=gs://bucket/path/config.yaml \
-- \
--template GENERAL \
--config config.yaml
```

### Start without exporting env vars

The environment variables can also be passed in via a single command without exporting, eg:
```
GCP_PROJECT=<project id> \
REGION=<region name> \
GCS_STAGING_LOCATION=<gcs path> \
SUBNET=projects/<project id>/regions/<region name>/subnetworks/<subnetwork name> \
bin/start_v2.sh \
--files=gs://bucket/path/config.yaml \
-- \
--template GENERAL \
--config config.yaml
```

# Logging

As a workaround for a logging issue, to enable your job's application logs (outside the spark packages)
you can provide a `log4j-spark-driver-template.properties` file in the `--files` spark argument:

Note: this is set automatically for all templates, but as this template overrides the `--files` flag
we must include it ourselves to enable logging.

eg:
``````
bin/start_v2.sh \
--files=gs://bucket/path/log4j-spark-driver-template.properties,gs://bucket/path/config.yaml \
-- \
--template GENERAL \
--config config.yaml
```

