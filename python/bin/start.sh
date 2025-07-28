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

PACKAGE_EGG_FILE=dist/dataproc_templates_distribution.egg

. ${BIN_DIR}/dataproc_template_functions.sh

check_required_envvar GCP_PROJECT
check_required_envvar REGION
check_required_envvar GCS_STAGING_LOCATION

# Remove trailing forward slash
GCS_STAGING_LOCATION=`echo $GCS_STAGING_LOCATION | sed 's/\/*$//'`

# Do not rebuild when SKIP_BUILD is specified
# Usage: export SKIP_BUILD=true
if [ -z "$SKIP_BUILD" ]; then
    python3 ${PROJECT_ROOT_DIR}/setup.py bdist_egg --output=$PACKAGE_EGG_FILE
fi

if [ $4 = "--template=HBASETOGCS" ]; then
  OPT_SPARK_VERSION="--version=1.0.29"
else
  OPT_SPARK_VERSION="--version=1.2"
fi

OPT_PROJECT="--project=${GCP_PROJECT}"
OPT_REGION="--region=${REGION}"
#OPT_JARS="--jars=file:///usr/lib/spark/external/spark-avro.jar"
OPT_JARS="--jars=file:///usr/lib/spark/connector/spark-avro.jar"
if [[ $OPT_SPARK_VERSION == *"=1.1"* ]]; then
  echo "Dataproc Serverless Runtime 1.1 or CLUSTER Job Type Detected"
	OPT_JARS="--jars=file:///usr/lib/spark/external/spark-avro.jar"
fi
OPT_LABELS="--labels=job_type=dataproc_template"
OPT_DEPS_BUCKET="--deps-bucket=${GCS_STAGING_LOCATION}"
OPT_PY_FILES="--py-files=${PROJECT_ROOT_DIR}/${PACKAGE_EGG_FILE}"

# Optional arguments
if [ -n "${SUBNET}" ]; then
  OPT_SUBNET="--subnet=${SUBNET}"
fi
if [ -n "${CLUSTER}" ]; then
  OPT_CLUSTER="--cluster=${CLUSTER}"
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
  OPT_PY_FILES="${OPT_PY_FILES},${PY_FILES}"
fi
if [ -n "${SPARK_PROPERTIES}" ]; then
  OPT_PROPERTIES="--properties=${SPARK_PROPERTIES}"
fi
if [ -z "${JOB_TYPE}" ]; then
  JOB_TYPE=SERVERLESS
fi
if [ -n "${SERVICE_ACCOUNT_NAME}" ]; then
  OPT_SERVICE_ACCOUNT_NAME="--service-account=${SERVICE_ACCOUNT_NAME}"
fi

#if Hbase catalog is passed, then required hbase dependency are copied to staging location and added to jars
if [ -n "${CATALOG}" ]; then
  echo "Downloading Hbase jar dependency"
  wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar
  wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-shaded-mapreduce/2.4.12/hbase-shaded-mapreduce-2.4.12.jar
  wget https://repo1.maven.org/maven2/org/apache/htrace/htrace-core4/4.2.0-incubating/htrace-core4-4.2.0-incubating.jar
  gsutil copy hbase-client-2.4.12.jar ${GCS_STAGING_LOCATION}/hbase-client-2.4.12.jar
  gsutil copy hbase-shaded-mapreduce-2.4.12.jar ${GCS_STAGING_LOCATION}/hbase-shaded-mapreduce-2.4.12.jar
  gsutil copy htrace-core4-4.2.0-incubating.jar ${GCS_STAGING_LOCATION}/htrace-core4-4.2.0-incubating.jar
  echo "Passing downloaded dependency jars"
  OPT_JARS="${OPT_JARS},${GCS_STAGING_LOCATION}/hbase-client-2.4.12.jar,${GCS_STAGING_LOCATION}/hbase-shaded-mapreduce-2.4.12.jar,${GCS_STAGING_LOCATION}/htrace-core4-4.2.0-incubating.jar,file:///usr/lib/spark/external/hbase-spark.jar"
  rm hbase-client-2.4.12.jar
  rm hbase-shaded-mapreduce-2.4.12.jar
  rm htrace-core4-4.2.0-incubating.jar
fi

if [ -n "${HBASE_SITE_PATH}" ]; then
  check_required_envvar SKIP_IMAGE_BUILD
  if [ "${SKIP_IMAGE_BUILD}" = "FALSE" ]; then
    echo "Building Custom Image"
    #Copy the hbase-site.xml to docker context
    cp $HBASE_SITE_PATH .
    export HBASE_SITE_NAME=`basename $HBASE_SITE_PATH`
    docker build -t "${IMAGE}" -f dataproc_templates/hbase/Dockerfile --build-arg HBASE_SITE_NAME=${HBASE_SITE_NAME} .
    rm $HBASE_SITE_NAME
    docker push "${IMAGE}"
  fi
fi

# Construct the command based on JOB_TYPE
if [ "${JOB_TYPE}" == "CLUSTER" ]; then
  echo "JOB_TYPE is CLUSTER, so will submit on an existing Dataproc cluster"
  check_required_envvar CLUSTER
  command=$(cat << EOF
  gcloud dataproc jobs submit pyspark \
      ${PROJECT_ROOT_DIR}/main.py \
      ${OPT_PROJECT} \
      ${OPT_REGION} \
      ${OPT_CLUSTER} \
      ${OPT_JARS} \
      ${OPT_LABELS} \
      ${OPT_FILES} \
      ${OPT_PY_FILES} \
      ${OPT_PROPERTIES}
EOF
)
elif [ "${JOB_TYPE}" == "SERVERLESS" ]; then
  echo "JOB_TYPE is SERVERLESS, so will submit on serverless Spark"
  command=$(cat << EOF
  gcloud dataproc batches submit pyspark \
      ${PROJECT_ROOT_DIR}/main.py \
      ${OPT_SPARK_VERSION} \
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
      ${OPT_METASTORE_SERVICE} \
      ${OPT_SERVICE_ACCOUNT_NAME}
EOF
)
else
  echo "Unknown JOB_TYPE \"${JOB_TYPE}\""
  exit 1
fi

echo "Triggering Spark Submit job"
echo ${command} "$@"
${command} "$@"
spark_status=$?

check_status $spark_status "\n Spark Command ran successful \n" "\n It seems like there are some issues in running spark command. Requesting you to please go through the error to identify issues in your code \n"

echo "We will love to hear your feedback at: https://forms.gle/XXCJeWeCJJ9fNLQS6"
echo "Email us at: dataproc-templates-support-external@googlegroups.com"
