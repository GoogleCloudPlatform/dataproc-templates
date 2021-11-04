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
   echo "start.sh [property=value]"
   echo "Required properties:"
   echo "GCP_PROJECT=<value>"
   echo "REGION=<value>"
   echo "SUBNET=<value>"
   echo "GCS_STAGING_BUCKET=<value>"
   echo "HISTORY_SERVER_CLUSTER=<value>"
   echo "TEMPLATE_NAME=<value>"
   echo
}


#Parse arguments of type property=value and initialize variable with property name
parse_arguments() {
  SET -x
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