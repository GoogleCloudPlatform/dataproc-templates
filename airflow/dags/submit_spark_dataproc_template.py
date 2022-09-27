"""
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""

import datetime
import os, configparser

from airflow import models
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.utils import dates

pwd = os.path.dirname(os.path.abspath(__file__))

# Read configuration variables
def read_configuration(config_file_path):
  full_path = os.path.join(pwd, config_file_path)
  config = configparser.ConfigParser(interpolation=None, allow_no_value=True)
  config.optionxform = str
  try:
    config.read(full_path)
    return config
  except configparser.Error as exc:
    raise AirflowFailException(exc)

config = read_configuration("config/submit_spark_dataproc_template.ini")

# Template Arguments
running_template = config['TEMPLATE_ARGS']['RUNNING_TEMPLATE']
template_args = ""
for arg, value in config.items(running_template):
  if arg == "template":
    template_args += "--" + arg + "=" + value + " "
  else:
    template_args += "--templateProperty " + arg + "=" + value + " "

# DAG arguments
default_dag_args = {
    'start_date': dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': int(config['COMPOSER']['RETRIES']),
    'execution_timeout': datetime.timedelta(seconds=int(config['COMPOSER']['EXEC_TIMEOUT'])),
    'dagrun_timeout': datetime.timedelta(seconds=int(config['COMPOSER']['RUN_TIMEOUT'])),
    'retry_delay': datetime.timedelta(seconds=int(config['COMPOSER']['RETRY_DELAY'])),
    'project_id': config['ENV_VARS']['GCP_PROJECT'],
    'region': config['ENV_VARS']['REGION']
}

# Env Vars
env_vars = dict(config.items('ENV_VARS'))
if("APPEND_PATH" in env_vars):
  env_vars["PATH"] = env_vars["APPEND_PATH"] + ":" + os.environ["PATH"]

# Create DAG
with models.DAG(
    dag_id=config['COMPOSER']['DAG_NAME'],
    schedule_interval=config['COMPOSER']['SCHEDULE_INTERVAL'] if config['COMPOSER']['SCHEDULE_INTERVAL'] else None,
    catchup=False,
    default_args=default_dag_args) as dag:

    # Run Dataproc Template
    run_dataproc_template = BashOperator(
        task_id="run_dataproc_template",
        bash_command="cd ./dependencies/dataproc_templates/java && bin/start.sh -- " + template_args,
        env=env_vars,
        cwd=dag.folder
    )

    # Define DAG dependencies.
    run_dataproc_template