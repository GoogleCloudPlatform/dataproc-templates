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

# Check if an environment variable is set,
# takes the name of the environment variable as an argument
check_required_envvar() {
  name=$1        # We pass in the variable to check as a string
  value=${!name} # Indirect expansion to get the value
  if [ -z "${value}" ]
  then
    echo "Required environment variable ${name} is missing"
    Help && exit 1
  else
    echo "${name}=${value}"
  fi
}

# Auxiliary Function to check the exit status passed as an argument
# This function is also responsible for printing the error message or success message based on the exit status
check_status()
{
  if [ "$1" -eq 0 ];
  then
    printf "$2"
  else
    printf "$3"
    exit 1
  fi
}


#Mandatory vs optional  specify
Help() {
  # Display Help
  help_text=$(cat << EndOfMessage
    Usage:

    # Environment variables
    export GCP_PROJECT=projectId
    export REGION=us-west1
    export GCS_STAGING_LOCATION=gs://bucket/path

    export JOB_TYPE=SERVERLESS|CLUSTER # Defaults to serverless

    # Required environment variables for CLUSTER mode
    export CLUSTER={clusterId}

    # Optional environment variables for SERVERLESS mode
    export SUBNET=projects/{projectId}/regions/{regionId}/subnetworks/{subnetId}
    export HISTORY_SERVER_CLUSTER=projects/{projectId}/regions/{regionId}/clusters/{clusterId}
    export METASTORE_SERVICE=projects/{projectId}/locations/{regionId}/services/{serviceId}

    Usage syntax:

    start.sh [sparkSubmitArgs] -- --template templateName [--templateProperty key=value] [extraArgs]

    eg:
    start.sh -- --template GCSTOBIGQUERY --templateProperty gcs.bigquery.input.location=gs://bucket/path/ (etc...)
EndOfMessage
)
  echo "${help_text}"
}


#Formatted print
echo_formatted() {
  echo "==============================================================="
  echo
  echo $*
  echo
  echo "==============================================================="
}
