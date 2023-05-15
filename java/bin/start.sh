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
echo "Script Started Execution"

java --version
java_status=$?

BIN_DIR="$(dirname "$BASH_SOURCE")"
source ${BIN_DIR}/dataproc_template_functions.sh
check_status $java_status "\n Java is installed, thus we are good to go \n" "\n Java is not installed on this machine, thus we need to install that first \n"

mvn --version
mvn_status=$?

check_status $mvn_status "\n Maven is installed, thus we are good to go \n" "\n Maven is not installed on this machine, thus we need to install that first \n"

PROJECT_ROOT_DIR=${BIN_DIR}/..
JAR_FILE=dataproc-templates-1.0-SNAPSHOT.jar
if [ -z "${JOB_TYPE}" ]; then
  JOB_TYPE=SERVERLESS
fi

. ${BIN_DIR}/dataproc_template_functions.sh

check_required_envvar GCP_PROJECT
check_required_envvar REGION
check_required_envvar GCS_STAGING_LOCATION

# Remove trailing forward slash
GCS_STAGING_LOCATION=`echo $GCS_STAGING_LOCATION | sed 's/\/*$//'`

# Do not rebuild when SKIP_BUILD is specified
# Usage: export SKIP_BUILD=true
if [ -z "$SKIP_BUILD" ]; then

  #Change PWD to root folder for Maven Build
  cd ${PROJECT_ROOT_DIR}
  mvn clean spotless:apply install -DskipTests
  build_status=$?

  check_status $build_status "\n Code build went successful, thus we are good to go \n" "\n We ran into some issues while building the jar file, seems like mvn clean install is not running fine \n"
 
  #Copy jar file to GCS bucket Staging folder
  echo_formatted "Copying ${PROJECT_ROOT_DIR}/target/${JAR_FILE} to  staging bucket: ${GCS_STAGING_LOCATION}/${JAR_FILE}"
  gsutil cp ${PROJECT_ROOT_DIR}/target/${JAR_FILE} ${GCS_STAGING_LOCATION}/${JAR_FILE}
  check_status $? "\n Commands to copy the project jar file to GCS Staging location went fine, thus we are good to go \n" "\n It seems like there is some issue in copying the project jar file to GCS Staging location \n"


fi

OPT_SPARK_VERSION="--version=1.1"
OPT_PROJECT="--project=${GCP_PROJECT}"
OPT_REGION="--region=${REGION}"
OPT_JARS="--jars=file:///usr/lib/spark/external/spark-avro.jar,${GCS_STAGING_LOCATION}/${JAR_FILE}"
OPT_LABELS="--labels=job_type=dataproc_template"
OPT_DEPS_BUCKET="--deps-bucket=${GCS_STAGING_LOCATION}"
OPT_CLASS="--class=com.google.cloud.dataproc.templates.main.DataProcTemplate"

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
if [ -n "${SPARK_PROPERTIES}" ]; then
  OPT_PROPERTIES="--properties=${SPARK_PROPERTIES}"
fi

#if Hbase catalog is passed, then required hbase dependency are copied to staging location and added to jars
if [ -n "${CATALOG}" ]; then
  echo "Downloading Hbase jar dependency"
  wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar
  wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-shaded-mapreduce/2.4.12/hbase-shaded-mapreduce-2.4.12.jar
  gsutil copy hbase-client-2.4.12.jar ${GCS_STAGING_LOCATION}/hbase-client-2.4.12.jar
  gsutil copy hbase-shaded-mapreduce-2.4.12.jar ${GCS_STAGING_LOCATION}/hbase-shaded-mapreduce-2.4.12.jar
  echo "Passing downloaded dependency jars"
  OPT_JARS="${OPT_JARS},${GCS_STAGING_LOCATION}/hbase-client-2.4.12.jar,${GCS_STAGING_LOCATION}/hbase-shaded-mapreduce-2.4.12.jar,file:///usr/lib/spark/external/hbase-spark.jar"
  rm hbase-client-2.4.12.jar
  rm hbase-shaded-mapreduce-2.4.12.jar
fi

if [ -n "${HBASE_SITE_PATH}" ]; then
  #Copy the hbase-site.xml to docker context
  cp $HBASE_SITE_PATH .
  export HBASE_SITE_NAME=`basename $HBASE_SITE_PATH`
  docker build -t "${IMAGE}" -f src/main/java/com/google/cloud/dataproc/templates/hbase/Dockerfile --build-arg HBASE_SITE_NAME=${HBASE_SITE_NAME} .
  rm $HBASE_SITE_NAME
  docker push "${IMAGE}"
fi

# Running on an existing dataproc cluster or run on serverless spark
if [ "${JOB_TYPE}" == "CLUSTER" ]; then
  echo "JOB_TYPE is CLUSTER, so will submit on existing dataproc cluster"
  check_required_envvar CLUSTER
  command=$(cat << EOF
  gcloud dataproc jobs submit spark \
      ${OPT_PROJECT} \
      ${OPT_REGION} \
      ${OPT_CLUSTER} \
      ${OPT_JARS} \
      ${OPT_LABELS} \
      ${OPT_PROPERTIES} \
      ${OPT_CLASS}
EOF
)
elif [ "${JOB_TYPE}" == "SERVERLESS" ]; then
  echo "JOB_TYPE is SERVERLESS, so will submit on serverless spark"
  command=$(cat << EOF
  gcloud beta dataproc batches submit spark \
      ${OPT_SPARK_VERSION} \
      ${OPT_PROJECT} \
      ${OPT_REGION} \
      ${OPT_JARS} \
      ${OPT_LABELS} \
      ${OPT_DEPS_BUCKET} \
      ${OPT_CLASS} \
      ${OPT_PROPERTIES} \
      ${OPT_SUBNET} \
      ${OPT_HISTORY_SERVER_CLUSTER} \
      ${OPT_METASTORE_SERVICE}
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

