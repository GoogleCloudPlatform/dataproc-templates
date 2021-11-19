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

#Initialize functions and Constants
BIN_DIR="$(dirname "$BASH_SOURCE")"
PROJECT_ROOT_DIR=${BIN_DIR}/..
. ${BIN_DIR}/dataproc_template_constants.sh
. ${BIN_DIR}/dataproc_template_functions.sh

#Parse Command Line arguments and check mandatory fields exist
parse_arguments $*
check_mandatory_fields GCP_PROJECT REGION SUBNET GCS_STAGING_BUCKET TEMPLATE_NAME


echo_formatted "Spark args are $SPARK_ARGS"

#Change PWD to root folder for Maven Build
cd ${PROJECT_ROOT_DIR}
mvn clean spotless:apply install -DskipTests
mvn dependency:get -Dartifact=io.grpc:grpc-grpclb:1.40.1 -Dmaven.repo.local=./grpc_lb

#Copy jar file to GCS bucket Staging folder
echo_formatted "Copying ${PROJECT_ROOT_DIR}/target/${JAR_FILE} to  staging bucket: ${GCS_STAGING_BUCKET}/${JAR_FILE}"
gsutil cp ${PROJECT_ROOT_DIR}/target/${JAR_FILE} ${GCS_STAGING_BUCKET}/${JAR_FILE}
gsutil cp ${PROJECT_ROOT_DIR}/grpc_lb/io/grpc/grpc-grpclb/1.40.1/grpc-grpclb-1.40.1.jar ${GCS_STAGING_BUCKET}/grpc-grpclb-1.40.1.jar

export JAR=file:///usr/lib/spark/external/spark-avro.jar,${GCS_STAGING_BUCKET}/grpc-grpclb-1.40.1.jar

temporary_fix_for_log_level

echo "Triggering Spark Submit job"

case ${JOB_TYPE} in

    "dataproc")
    echo_formatted "
           gcloud beta dataproc batches submit spark \
          --project=${GCP_PROJECT} \
          --region=${REGION} \
          --cluster ${CLUSTER} \
          --jars=${JAR},${GCS_STAGING_BUCKET}/${JAR_FILE} \
          --labels job_type=dataproc_template \
          $SPARK_ARGS \
          --class com.google.cloud.dataproc.templates.main.DataProcTemplate \
          -- ${TEMPLATE_NAME} $ARGS
        "

        gcloud  dataproc jobs submit spark \
        --project=${GCP_PROJECT} \
        --region=${REGION} \
        --cluster=${CLUSTER} \
        --jars=${JAR},${GCS_STAGING_BUCKET}/${JAR_FILE} \
        --labels job_type=dataproc_template \
        $SPARK_ARGS \
        --class com.google.cloud.dataproc.templates.main.DataProcTemplate \
        -- ${TEMPLATE_NAME} $ARGS
    ;;

    "serverless")

    echo_formatted "
       gcloud beta dataproc batches submit spark \
      --project=${GCP_PROJECT} \
      --region=${REGION} \
      --subnet ${SUBNET} \
      --jars=${JAR},${GCS_STAGING_BUCKET}/${JAR_FILE} \
      --labels job_type=dataproc_template \
      --deps-bucket=${GCS_STAGING_BUCKET} \
      $SPARK_ARGS \
      --class com.google.cloud.dataproc.templates.main.DataProcTemplate \
      -- ${TEMPLATE_NAME} $ARGS
    "

    gcloud beta dataproc batches submit spark \
    --project=${GCP_PROJECT} \
    --region=${REGION} \
    --subnet ${SUBNET} \
    --jars=${JAR},${GCS_STAGING_BUCKET}/${JAR_FILE} \
    --labels job_type=dataproc_template \
    --deps-bucket=${GCS_STAGING_BUCKET} \
    $SPARK_ARGS \
    --class com.google.cloud.dataproc.templates.main.DataProcTemplate \
    -- ${TEMPLATE_NAME} $ARGS

    ;;

    *)
      echo "Unidentified job type"
      exit ${FAILURE_CODE}
  esac




