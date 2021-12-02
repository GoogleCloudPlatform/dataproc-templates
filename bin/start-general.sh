#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -e

# Project and cluster settings
PROJECT= # projectId
REGION=us-west1 # us-west1
SUBNET= # projects/projectId/regions/us-west1/subnetworks/subnet
HISTORY_SERVER_CLUSTER= # projects/projectId/regions/region/clusters/clusterId
METASTORE_SERVICE= # projects/projectId/locations/region/services/serviceId
GCS_DEPS_BUCKET= # gs://bucket/path

JAR_FILE=dataproc-templates-1.0-SNAPSHOT.jar
LOG4J_PROPERTIES_FILE=src/test/resources/log4j-custom.properties

# Change this to point to your config.yaml
CONFIG_FILE=src/test/resources/config-example.yaml

mvn clean spotless:apply install -DskipTests
gsutil cp "target/${JAR_FILE}" "${GCS_DEPS_BUCKET}/jars/${JAR_FILE}"
gsutil cp "${LOG4J_PROPERTIES_FILE}" "${GCS_DEPS_BUCKET}/config/log4j-custom.properties"
gsutil cp "${CONFIG_FILE}" "${GCS_DEPS_BUCKET}/config/config.yaml"

gcloud beta dataproc batches submit spark \
  --project=${PROJECT} --region=${REGION} --subnet ${SUBNET} \
  --jars=file:///usr/lib/spark/external/spark-avro.jar,${GCS_DEPS_BUCKET}/jars/${JAR_FILE} \
  --labels job_type=dataproc_template \
  --deps-bucket=${GCS_DEPS_BUCKET}/ \
  --metastore-service=${METASTORE_SERVICE} \
  --history-server-cluster=${HISTORY_SERVER_CLUSTER} \
  --files=${GCS_DEPS_BUCKET}/config/config.yaml,${GCS_DEPS_BUCKET}/config/log4j-custom.properties \
  --properties=spark.driver.extraJavaOptions='-Dlog4j.configuration=file:log4j-custom.properties' \
  --class com.google.cloud.dataproc.templates.GeneralTemplate \
  -- --config config.yaml
