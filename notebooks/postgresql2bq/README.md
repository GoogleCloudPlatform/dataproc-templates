## Jupyter Notebook Solution for migrating PostgreSQL to BigQuery DWH using Dataproc Templates


This notebook solution utilizes dataproc templates for migrating databases from PostgreSQL to BIGQUERY. 

The notebook contains step by step process for a downtime based migration.

Refer [Setup Vertex AI - PySpark](../generic_notebook/README.md) to setup new Jupyter notebook in vertexAI.
Once the setup is done navigate to `/notebooks/postgresql2bq` folder and open
[postgresql-to-bigquery-notebook](./postgresql-to-bigquery-notebook.ipynb).

### Overview

This notebook is built on top of:
* [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks)
* [Google Cloud's Dataproc Serverless](https://cloud.google.com/dataproc-serverless/)
* Dataproc Templates which are maintained in this github project.

### Key Benefits
1) Automatically generates list of tables from metadata. Alternatively, user can supply list of tables.
2) Identifies current primary key column name, and partitioned read properties.
3) Automatically uses partition reads if exceeds threshold.
4) Divides migration into batches and parallely migrates multiple tables.
5) Notebook allows you to choose modes i.e. appending data or overwrite.
6) Bigquery load automatically creates table if the table does not exists.


### Requirements

Below configurations are required before proceeding further.

#### Common Parameters

* `PROJECT` : GCP project-id
* `REGION` : GCP region
* `GCS_STAGING_LOCATION` : Cloud Storage staging location to be used for this notebook to store artifacts
* `SUBNET` : VPC subnet
* `JARS` : list of jars. For this notebook postgresql and Spark Bigquery connector jars are required in addition to the dataproc template jars
* `MAX_PARALLELISM` : Parameter for number of jobs to run in parallel default value is 5

#### POSTGRESQL  Parameters
* `POSTGRESQL_HOST` : POSTGRESQL instance ip address
* `POSTGRESQL_PORT` : POSTGRESQL instance port
* `POSTGRESQL_USERNAME` : POSTGRESQL username
* `POSTGRESQL_PASSWORD` : POSTGRESQL password
* `POSTGRESQL_DATABASE` : Name of database that you want to migrate
* `POSTGRESQL_TABLE_LIST` : List of tables you want to migrate eg ['schema.table1','schema.table2'] else provide an empty list for migration of specific schemas or the whole database. Example: [].
* `POSTGRESQL_SCHEMA_LIST` : List of schemas. Use this if you'ld like to migrate all tables associated with specific schemas eg. ['schema1','schema2']. Otherwise, leave this parameter empty. Example: []. This comes in handy when don't want to provide names of all the tables separately but would rather prefer migrating all tables from a schema.

#### BigQuery Parameters

* `BIGQUERY_DATASET` : BigQuery Target Dataset
* `BIGQUERY_MODE` : Mode of operation at target append/overwrite




### Run programmatically with parameterize script

Alternatively to running the notebook manually, we developed a "parameterize" script, using the papermill lib, to allow running notebooks programmatically from a Python script, with parameters.  

**Example submission:**
```
export GCP_PROJECT=<project>
export REGION=<region>
export GCS_STAGING_LOCATION=<bucket-name>
export SUBNET=<subnet>

python run_notebook.py --script=POSTGRESQLTOBIGQUERY \
                        --postgresql.host="10.x.x.x" \
                        --postgresql.port="3306" \
                        --postgresql.username="user" \
                        --postgresql.password="password" \
                        --postgresql.database="db" \
                        --postgresql.table.list="employee" \
                        --postgresql.schema.list="" \

                        --bigquery.dataset="templates" \
                        --bigquery.mode.="overwrite"
```

**Parameters:**
```
python run_notebook.py --script=POSTGRESQLTOBIGQUERY --help

usage: run_notebook.py [-h]
        [--output.notebook OUTPUT.NOTEBOOK]
        --postgresql.host POSTGRESQL_HOST
        [--postgresql.port POSTGRESQL_PORT]
        --postgresql.username POSTGRESQL_USERNAME
        --postgresql.password POSTGRESQL_PASSWORD
        --postgresql.database POSTGRESQL_DATABASE
        [--postgresql.table.list POSTGRESQL_TABLE_LIST]
        [--postgresql.schema.list POSTGRESQL_SCHEMA_LIST]
        --bigquery.dataset BIGQUERY_DATASET
        --bigquery.mode BIGQUERY_MODE

optional arguments:
    -h, --help            
        show this help message and exit
    --output.notebook OUTPUT.NOTEBOOK
        Path to save executed notebook (Default: None). If not provided, no notebook is saved
    --postgresql.host POSTGRESQL_HOST
        POSTGRESQL host or IP address
    --postgresql.port POSTGRESQL_PORT
        POSTGRESQL port (Default: 3306)
    --postgresql.username POSTGRESQL_USERNAME
        POSTGRESQL username
    --postgresql.password POSTGRESQL_PASSWORD
        POSTGRESQL password
    --postgresql.database POSTGRESQL_DATABASE
        POSTGRESQL database name
    --postgresql.table.list POSTGRESQL_TABLE_LIST
        POSTGRESQL table list to migrate. Leave empty for migrating complete database else provide tables as "table1,table2"
    --postgresql.table.list POSTGRESQL_SCHEMA_LIST
        POSTGRESQL schema list to migrate. Only Migrate tables associated with the provided schema list
    --bigquery.dataset BIGQUERY_DATASET
        BIGQUERY dataset name
    --bigquery.dataset BIGQUERY_MODE
        BIGQUERY output write mode (Default: overwrite)

```

### Required JAR files

This notebook requires the POSTGRESSQL connector jars. Installation information is present in the notebook




