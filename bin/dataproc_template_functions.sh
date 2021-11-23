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


#Mandatory vs optional  specify
Help() {
# Display Help
   echo "Usage Syntax:"
   echo
   echo "Running Templates in Serverless Spark:"
   echo "start.sh [property=value]"
   echo "Required properties:"
   echo "GCP_PROJECT=<value>"
   echo "REGION=<value>"
   echo "SUBNET=<value>"
   echo "GCS_STAGING_BUCKET=<value>"
   echo "TEMPLATE_NAME=<value>"
   echo
   echo  "Optional properties"
   echo  "HISTORY_SERVER_CLUSTER=<value>"
   echo
   echo "Running Templates in existing dataproc cluster:"
   echo "start.sh [property=value]"
   echo "Required properties:"
   echo "GCP_PROJECT=<value>"
   echo "CLUSTER=<value>"
   echo "REGION=<value>"
   echo "JOB_TYPE=dataproc"
   echo "GCS_STAGING_BUCKET=<value>"
   echo "TEMPLATE_NAME=<value>"
   echo

}


#Parse arguments of type property=value and initialize variable with property name
parse_arguments() {
  ARGS=""
  SPARK_ARGS=""
  echo "Arugments passed to script : $*"
  IFS=' ' read -r -a array <<< "$*"
  for element in "${array[@]}"
  do
    if [[ "$element" == --* ]]
    then
     SPARK_ARGS=$SPARK_ARGS$element" "
    elif [[ "$element" =~ "=" ]]
    then
     echo "Set $element"
     eval "$element"
     else
       ARGS=$ARGS:element
    fi

  done
  #If optional properties exist add them as spark arguments
  #Skip adding history server cluster if target is dataproc cluster
  if [[ ! -z "${HISTORY_SERVER_CLUSTER}" && "${JOB_TYPE}" == "${SERVERLESS_JOB_CODE}" ]]
  then
    SPARK_ARGS=$SPARK_ARGS$" --history-server-cluster=${HISTORY_SERVER_CLUSTER} "
    fi
}

#Verify the variables passed are non-empty
check_mandatory_fields(){
  var_names=("$@")
    for var_name in "${var_names[@]}"; do
        if [ -z "${!var_name}" ]
         then
           echo "ERROR: Required property $var_name is missing"
           echo
           Help
           exit 1
         fi
    done
}


#Formatted print
echo_formatted() {
  echo "==============================================================="
  echo
  echo $*
  echo
  echo "==============================================================="
}

 #Use custom log4j.properties file
temporary_fix_for_log_level() {
  log_property_string="--properties=spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j-spark-driver-template.properties"
  if [ -z "${SPARK_ARGS}" ]
         then
            SPARK_ARGS=" ${log_property_string}"
           elif [[ "${SPARK_ARGS}" =~ "--properties" ]]
           then
             SPARK_ARGS=`echo ${SPARK_ARGS} | sed -e "s/--properties=/${log_property_string},/g"`
            else
              SPARK_ARGS=${SPARK_ARGS}" ${log_property_string}"
           fi

}