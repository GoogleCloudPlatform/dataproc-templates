# Airflow Orchestration of Dataproc Templates

Content in this folder demonstrates how to run Dataproc Templates from DAGs running in Airflow

#### Deploy - Structure your Airflow dags folder following this structure:
```
dags/
    |_submit_pyspark_dataproc_template.py
    |_submit_spark_dataproc_template.py
    |_config/
            |_submit_pyspark_dataproc_template.ini
            |_submit_spark_dataproc_template.ini
    |_dependencies/
            |_dataproc_templates/
                    |_python/   # Dataproc Templates python folder
                    |_java/     # Dataproc Templates java folder
```

To do that, after following the configuration instructions below, you can use the following instructions:

```
#### If using Composer as your Airflow environment
export DAGS_FOLDER=$(gcloud composer environments describe <your_composer_env_name> \  --location <your_location> \  --format="get(config.dagGcsPrefix)")

#### If using you Local Machine as your Airflow environment
export DAGS_FOLDER=~/airflow/dags/

#### Copy the files to the dags folder
cp -r airflow/pyspark/dags/* $DAGS_FOLDER/
mkdir $DAGS_FOLDER/dependencies/
cp -r python $DAGS_FOLDER/dependencies/dataproc_templates/
cp -r java $DAGS_FOLDER/dependencies/dataproc_templates/
```

#### Setup - Configure Airflow execution
Update submit_pyspark_dataproc_template.ini or submit_spark_dataproc_template.ini with the desired Airflow configuration.  
Check [Airflow's documentation](https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html#dag-runs) to see the available options of the SCHEDULE_INTERVAL parameter.  
If None, leave it black, as shown below.
```
[COMPOSER]
DAG_NAME=submit_spark_dataproc_template
SCHEDULE_INTERVAL=
EXEC_TIMEOUT=600
RUN_TIMEOUT=600
RETRY_DELAY=60
RETRIES=1
```

#### Setup - Running a PySpark (Python) Dataproc Templates

Update submit_pyspark_dataproc_template.ini with the desired template name and its arguments, and the environment variables.

```
[ENV_VARS]
GCP_PROJECT=<your_project
REGION=<your_region>
GCS_STAGING_LOCATION=<gs://your_location>
SUBNET=<your_subnet>
JARS=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

[TEMPLATE_ARGS]
RUNNING_TEMPLATE=<YOUR_DESIRED_TEMPLATED>

[<YOUR_DESIRED_TEMPLATED>]
template=<YOUR_DESIRED_TEMPLATED>
your.template.first.argument=<value>
your.template.second.argument=<value>
your.template.third.argument=<value>
your.template.fourth.argument=<value>
```

#### Setup - Running a Spark (Java) Dataproc Templates

Update submit_pyspark_dataproc_template.ini with the desired template name and its arguments, and the environment variables.  
For Java Dataproc Templates, you have to set additional environment variables in the .ini configuration specifying your java and mvn locations.

```
[ENV_VARS]
GCP_PROJECT=<your_project
REGION=<your_region>
GCS_STAGING_LOCATION=<gs://your_location>
SUBNET=<your_subnet>
JARS=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
JAVA_HOME=/usr/lib/jvm/java-<version>-openjdk-amd64/
MAVEN_HOME=/usr/local/apache-maven/apache-maven-<version>
M2_HOME=/usr/local/apache-maven/apache-maven-<version>
APPEND_PATH=/usr/local/apache-maven/apache-maven-<version>/bin:/usr/lib/jvm/java-<version>-openjdk-amd64/bin

[TEMPLATE_ARGS]
RUNNING_TEMPLATE=<YOUR_DESIRED_TEMPLATED>

[<YOUR_DESIRED_TEMPLATED>]
template=<YOUR_DESIRED_TEMPLATED>
your.template.first.argument=<value>
your.template.second.argument=<value>
your.template.third.argument=<value>
your.template.fourth.argument=<value>
```

#### Run the unit tests
```
pytest airflow/pyspark
```

#### Done!!! Trigger the DAG in Airflow or wait for the schedule

### Google Cloud's Composer: your managed Airflow instance in production

Follow [this](https://cloud.google.com/composer/docs/composer-2/create-environments) documentation to create a Composer environment in GCP.  
After it is created, you can push your files to its _dags_ folder as mentioned in the first instruction.
