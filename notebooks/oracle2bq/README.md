## Jupyter Notebook Solution for migrating Oracle database to BigQuery using Dataproc Templates

Notebook solution utilizing dataproc templates for migrating databases from Oracle to BigQuery. Notebook contains step by step process for migrating Oracle database tables to BigQuery.

Refer [Setup Vertex AI - PySpark](./../generic_notebook/README.md) to setup new Jupyter notebook in vertexAI. Once the setup is done navigate to
[dataproc-templates/python/notebooks/oracle2bq](.) folder and open [OracleToBigQuery_notebook.ipynb](./OracleToBigQuery_notebook.ipynb) notebook.

### Overview

[OracleToBigQuery_notebook.ipynb](./OracleToBigQuery_notebook.ipynb) notebook solution is built on top of [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks) and [Google Cloud's Dataproc](https://cloud.google.com/dataproc/) tech stack provided by GCP.

### Key Benefits

* Automatically Generate list of tables from metadata. Alternatively, user should be able to supply list of tables.
* Identify current primary key column name, and partitioned read properties.
* Automatically uses partition reads if exceeds threshold.
* Divides migration into batches and parallely migrates multiple tables.
* Notebook allow you to choose modes i.e. appending data or overwrite.
* BigQuery load automatically created table if table does not exists.

### Requirements

Below configurations are required before proceeding further.

#### Common Parameters

* `PROJECT` : GCP project-id
* `REGION` : GCP region
* `GCS_STAGING_LOCATION` : Cloud Storage staging location to be used for this notebook to store artifacts (gs://bucket-name)
* `SUBNET` : VPC subnet
* `JARS` : list of jars. For this notebook Oracle driver and BigQuery connector with the Dataproc template jars
* `MAX_PARALLELISM` : Parameter for number of jobs to run in parallel default value is 2

#### Oracle Parameters

* `ORACLE_HOST` : Oracle instance ip address
* `ORACLE_PORT` : Oracle instance port
* `ORACLE_USERNAME` : Oracle username
* `ORACLE_PASSWORD` : Oracle password
* `ORACLE_DATABASE` : Name of database/service for Oracle connection
* `ORACLE_SCHEMA` : Schema to be exported, leave blank to export tables owned by ORACLE_USERNAME
* `ORACLE_TABLE_LIST` : List of tables you want to migrate eg: ['table1','table2'] else provide empty list for migration whole database eg : []

#### BigQuery Parameters

* `BIGQUERY_DATASET` : BigQuery Target Dataset
* `BIGQUERY_MODE` : Mode of operation at target append/overwrite
* `TEMP_GCS_BUCKET` : Cloud Storage bucket to be used for temporary staging

### Run programmatically with parameterize script

Alternatively to running the notebook manually, we developed a "parameterize" script, using the papermill lib, to allow running notebooks programmatically from a Python script, with parameters.

**Example submission:**

```shell
export GCP_PROJECT=<project>
export REGION=<region>
export GCS_STAGING_LOCATION=gs://<bucket-name>
export SUBNET=<subnet>
export SERVICE_ACCOUNT=<service-account>

python run_notebook.py --script=ORACLETOBIGQUERY \
                        --oracle.host="10.x.x.x" \
                        --oracle.port="3306" \
                        --oracle.username="user" \
                        --oracle.password="password" \
                        --oracle.database="db" \
                        --oracle.table.list="employee" \
                        --bigquery.dataset="bq-dataset" \
                        --temp.gcs.bucket="my-bucket"
```

**Parameters:**

```
python run_notebook.py --script=ORACLETOBIGQUERY --help
usage: run_notebook.py [-h] --oracle.host ORACLE_HOST [--oracle.port ORACLE_PORT] --oracle.username ORACLE_USERNAME --oracle.password
                       ORACLE_PASSWORD --oracle.database ORACLE_DATABASE [--oracle.schema ORACLE_SCHEMA] [--oracle.table.list ORACLE_TABLE_LIST]
                       [--bigquery.mode {overwrite,append}] --bigquery.dataset BIGQUERY_DATASET --temp.gcs.bucket TEMP_GCS_BUCKET
                       [--max.parallelism MAX_PARALLELISM] [--output.notebook OUTPUT.NOTEBOOK]

optional arguments:
  -h, --help            show this help message and exit
  --output.notebook OUTPUT.NOTEBOOK
                        Path to save executed notebook (Default: None). If not provided, no notebook is saved
  --oracle.host ORACLE_HOST
                        Oracle host or IP address
  --oracle.port ORACLE_PORT
                        Oracle port (Default: 1521)
  --oracle.username ORACLE_USERNAME
                        Oracle username
  --oracle.password ORACLE_PASSWORD
                        Oracle password
  --oracle.database ORACLE_DATABASE
                        Oracle database name
  --oracle.schema ORACLE_SCHEMA
                        Schema to be exported, leave blank to export tables owned by ORACLE_USERNAME
  --oracle.table.list ORACLE_TABLE_LIST
                        Oracle table list to migrate. Leave empty for migrating complete database else provide tables as "table1,table2"
  --bigquery.mode {overwrite,append}
                        BigQuery output write mode (Default: overwrite). Use append when schema already exists in BigQuery
  --bigquery.dataset BIGQUERY_DATASET
                        BigQuery dataset name
  --temp.gcs.bucket TEMP_GCS_BUCKET
                        Temporary staging Cloud Storage bucket name
  --max.parallelism MAX_PARALLELISM
                        Maximum number of tables that will migrated parallelly (Default: 5)
```

### Required JAR files

This notebook requires the Oracle driver and BigQuery Connector jar. Installation information is present in the notebook
