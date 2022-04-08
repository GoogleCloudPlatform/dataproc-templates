#!/usr/bin/env bash
set -e
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

#Initialize functions and Constants
BIN_DIR="$(dirname "$BASH_SOURCE")"
PROJECT_ROOT_DIR=${BIN_DIR}/..

PACKAGE_EGG_FILE=dist/dataproc_templates-0.0.1-py3.8.egg

. ${BIN_DIR}/dataproc_template_functions.sh

check_required_envvar GCP_PROJECT
check_required_envvar REGION
check_required_envvar GCS_STAGING_LOCATION

python ${PROJECT_ROOT_DIR}/setup.py bdist_egg

OPT_PROJECT="--project=${GCP_PROJECT}"
OPT_REGION="--region=${REGION}"
OPT_JARS="--jars=file:///usr/lib/spark/external/spark-avro.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
OPT_LABELS="--labels=job_type=dataproc_template"
OPT_DEPS_BUCKET="--deps-bucket=${GCS_STAGING_LOCATION}"
OPT_PY_FILES="--py-files=${PROJECT_ROOT_DIR}/${PACKAGE_EGG_FILE}"

# Optional arguments
if [ -n "${SUBNET}" ]; then
  OPT_SUBNET="--subnet=${SUBNET}"
fi
if [ -n "${HISTORY_SERVER_CLUSTER}" ]; then
  OPT_HISTORY_SERVER_CLUSTER="--history-server-cluster=${HISTORY_SERVER_CLUSTER}"
fi
if [ -n "${METASTORE_SERVICE}" ]; then
  OPT_METASTORE_SERVICE="--metastore-service=${METASTORE_SERVICE}"
fi
if [ -n "${JARS}" ]; then
  OPT_JARS="${OPT_JARS},${JARS}"
fi
if [ -n "${FILES}" ]; then
  OPT_FILES="--files=${FILES}"
fi
if [ -n "${PY_FILES}" ]; then
  OPT_FILES="${OPT_PY_FILES},${PY_FILES}"
fi


command=$(cat << EOF
gcloud beta dataproc batches submit pyspark \
    ${PROJECT_ROOT_DIR}/main.py \
    ${OPT_PROJECT} \
    ${OPT_REGION} \
    ${OPT_JARS} \
    ${OPT_LABELS} \
    ${OPT_DEPS_BUCKET} \
    ${OPT_FILES} \
    ${OPT_PY_FILES} \
    ${OPT_PROPERTIES} \
    ${OPT_SUBNET} \
    ${OPT_HISTORY_SERVER_CLUSTER} \
    ${OPT_METASTORE_SERVICE}
EOF
)

echo "Triggering Spark Submit job"
echo ${command} "$@"
${command} "$@"
