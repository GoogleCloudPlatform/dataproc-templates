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

. ${BIN_DIR}/dataproc_template_functions.sh

check_required_envvar GCP_PROJECT
check_required_envvar REGION
check_required_envvar GCS_STAGING_LOCATION
check_required_envvar SUBNET
if [ -z "$1" ];
  then
    Help
    echo
    echo "No template argument supplied"
    exit
fi

#Change PWD to root folder
cd ${PROJECT_ROOT_DIR}

OPT_PROJECT="--project=${GCP_PROJECT}"
OPT_REGION="--region=${REGION}"
OPT_JARS="--jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
OPT_LABELS="--labels=job_type=dataproc_template"
OPT_DEPS_BUCKET="--deps-bucket=${GCS_STAGING_LOCATION}"
OPT_PROPERTIES=""
OPT_SUBNET="--subnet=${SUBNET}"
OPT_FILES="--files=src/templates/util/template_constants.py"
OPT_PY_FILES="--py-files=src/templates/resources/default_args.ini"
TEMPLATE_FILE="src/templates/$1.py"

echo "Submitting on serverless spark"
command=$(cat << EOF
  gcloud dataproc batches submit pyspark \
      ${OPT_PROJECT} \
      ${OPT_REGION} \
      ${OPT_JARS} \
      ${OPT_LABELS} \
      ${OPT_DEPS_BUCKET} \
      ${OPT_PROPERTIES} \
      ${OPT_SUBNET} \
      ${OPT_FILES} \
      ${OPT_PY_FILES} \
      ${OPT_HISTORY_SERVER_CLUSTER} \
      ${OPT_METASTORE_SERVICE} \
      ${TEMPLATE_FILE}
EOF
)

echo "Triggering Spark Submit job"
echo ${command} "${@:2}"
${command} "${@:2}"
