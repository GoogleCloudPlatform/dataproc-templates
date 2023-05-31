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
* `GCS_STAGING_LOCATION` : GCS staging location to be used for this notebook to store artifacts
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

python run_notebook.py --script=MYSQLTOSPANNER \
                        --mysql.host="10.x.x.x" \
                        --mysql.port="3306" \
                        --mysql.username="user" \
                        --mysql.password="password" \
                        --mysql.database="db" \
                        --mysql.table.list="employee" \
                        --spanner.instance="spark-instance" \
                        --spanner.database="spark-db" \
                        --spanner.table.primary.keys="{\"employee\":\"empno\"}"
```

**Parameters:**
```
python run_notebook.py --script=MYSQLTOSPANNER --help

usage: run_notebook.py [-h]
        [--output.notebook OUTPUT.NOTEBOOK]
        --mysql.host MYSQL_HOST
        [--mysql.port MYSQL_PORT]
        --mysql.username MYSQL_USERNAME
        --mysql.password MYSQL_PASSWORD
        --mysql.database MYSQL_DATABASE
        [--mysql.table.list MYSQLTABLE_LIST]
        [--mysql.output.spanner.mode {overwrite,append}]
        --spanner.instance SPANNER_INSTANCE
        --spanner.database SPANNER_DATABASE
        --spanner.table.primary.keys SPANNER_TABLE_PRIMARY_KEYS
        [--max.parallelism MAX_PARALLELISM]

optional arguments:
    -h, --help            
        show this help message and exit
    --output.notebook OUTPUT.NOTEBOOK
        Path to save executed notebook (Default: None). If not provided, no notebook is saved
    --mysql.host MYSQL_HOST
        MySQL host or IP address
    --mysql.port MYSQL_PORT
        MySQL port (Default: 3306)
    --mysql.username MYSQL_USERNAME
        MySQL username
    --mysql.password MYSQL_PASSWORD
        MySQL password
    --mysql.database MYSQL_DATABASE
        MySQL database name
    --mysql.table.list MYSQLTABLE_LIST
        MySQL table list to migrate. Leave empty for migrating complete database else provide tables as "table1,table2"
    --mysql.output.spanner.mode {overwrite,append}
        Spanner output write mode (Default: overwrite). Use append when schema already exists in Spanner
    --spanner.instance SPANNER_INSTANCE
        Spanner instance name
    --spanner.database SPANNER_DATABASE
        Spanner database name
    --spanner.table.primary.keys SPANNER_TABLE_PRIMARY_KEYS
        Provide table & PK column which do not have PK in MySQL table {"table_name":"primary_key"}
    --max.parallelism MAX_PARALLELISM
        Maximum number of tables that will migrated parallelly (Default: 5)
```

### Required JAR files

This notebook requires the POSTGRESSQL connector jars. Installation information is present in the notebook


### Limitations:

* Does not work with Cloud Spanner's Postgresql Interface



