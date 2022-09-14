# Airflow Orchestration of Dataproc Templates

Content in this folder demonstrates how to run Dataproc Templates from DAGs running in Airflow

1) Structure your Airflow dags folder following this structure:
```
dags/
    |_submit_dataproc_template_serverless.py
    |_config/
            |_submit_dataproc_template_serverless.ini
    |_dependencies/
            |_dataproc_templates/
                    |_python/   # Dataproc Templates python folder
                    |_java/     # Dataproc Templates java folder
```

2) Update submit_dataproc_template_serverless.ini with the desired template name and its arguments:

```
[TEMPLATE_ARGS]
RUNNING_TEMPLATE=<YOUR_DESIRED_TEMPLATED>

[<YOUR_DESIRED_TEMPLATED>]
template=<YOUR_DESIRED_TEMPLATED>
your.template.first.argument=<value>
your.template.second.argument=<value>
your.template.third.argument=<value>
your.template.fourth.argument=<value>
```

3) Run the unit tests:
```
pytest airflow/pyspark
```

4) See the DAG in Airflow with id: **submit_dataproc_template_serverless**

### Tip: Use Composer as your managed Airflow instance in production

Follow [this](https://cloud.google.com/composer/docs/composer-2/create-environments) documentation to create a Composer environment in GCP.  
After it is created, you can push your files to its _dags_ folder:

```
export DAGS_FOLDER=$(gcloud composer environments describe <your_composer_env_name> \  --location <your_location> \  --format="get(config.dagGcsPrefix)")

cp -r airflow/pyspark/dags/* $DAGS_FOLDER/
cp -r python $DAGS_FOLDER/dependencies/dataproc_templates/
cp -r java $DAGS_FOLDER/dependencies/dataproc_templates/
```