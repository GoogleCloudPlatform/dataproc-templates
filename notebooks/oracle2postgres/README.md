## Jupyter Notebook Solution for migrating Oracle to POSTGRES Database using Dataproc Templates

Notebook solution utilizing dataproc templates for migrating databases from ORACLE to POSTGRES. 
Notebook contains step by step process for a downtime based migration.

Refer [Setup Vertex AI - PySpark](../generic_notebook/README.md) to setup new Jupyter notebook in vertexAI.
Once the setup is done navigate to `/notebooks/oracle2postgres` folder and open
[oracle-to-postgres-notebook](./OracleToPostgres_notebook.ipynb).

### Overview

This notebook is built on top of:
* [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks)
* [Google Cloud's Dataproc Serverless](https://cloud.google.com/dataproc-serverless/)
* Dataproc Templates which are maintained in this github project.

### Key Benefits
1) Automatically discovers all the Oracle tables.
2) Can automatically generates table schema in Postgresql, corresponding to each table.
3) Divides the migration into multiple batches and automatically computes metadata.
4) Parallely migrates mutiple Oracle tables to Postgresql.
5) Simple, easy to use and customizable.

### Requirements

Below configurations are required before proceeding further.
#### Common Parameters

* `PROJECT` : GCP project-id
* `REGION` : GCP region
* `GCS_STAGING_LOCATION` : Cloud Storage staging location to be used for this notebook to store artifacts
* `SUBNET` : VPC subnet
* `JARS` : list of jars. For this notebook oracle and postgres connector jars are required in addition with the dataproc template jars
* `MAX_PARALLELISM` : Parameter for number of jobs to run in parallel default value is 2

#### Oracle Parameters
* `ORACLE_HOST` : Oracle instance ip address
* `ORACLE_PORT` : Oracle instance port
* `ORACLE_USERNAME` : Oracle username
* `ORACLE_PASSWORD` : Oracle password
* `ORACLE_DATABASE` : Name of database/service for Oracle connection
* `ORACLE_TABLE_LIST` : List of tables you want to migrate eg: ['table1','table2'] else provide empty list for migration whole database eg : []

#### POSTGRES Parameters
* `POSTGRES_HOST` : Postgres instance ip address
* `POSTGRES_PORT` : Postgres instance port
* `POSTGRES_USERNAME` : Postgres username
* `POSTGRES_PASSWORD` : Postgres password
* `POSTGRES_DATABASE` : name of database that you want to migrate to
* `POSTGRES_SCHEMA` : Postgres Schema
* `OUTPUT_MODE` : Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to overwrite)
* `BATCH_SIZE` : JDBC output batch size. Default set to 1000

### Run programmatically with parameterize script

Alternatively to running the notebook manually, we developed a "parameterize" script, using the papermill lib, to allow running notebooks programmatically from a Python script, with parameters.

**Example submission:**

```shell
export GCP_PROJECT=<project>
export REGION=<region>
export GCS_STAGING_LOCATION=gs://<bucket-name>
export SUBNET=<subnet>
export SERVICE_ACCOUNT=<service-account> (optional)
python run_notebook.py --script=ORACLETOPOSTGRES \
                        --oracle.host="10.x.x.x" \
                        --oracle.port="3306" \
                        --oracle.username="user" \
                        --oracle.password="password" \
                        --oracle.database="db" \
                        --oracle.table.list="employee" \
                        --postgres.host="10.x.x.x" \
                        --postgres.port="3306" \
                        --postgres.username="user" \
                        --postgres.password="password" \
                        --postgres.database="db" \
                        --postgres.schema="employee" \

```

**Parameters:**

```
python run_notebook.py --script=ORACLETOPOSTGRES --help
usage: run_notebook.py [-h] [--output.notebook OUTPUT.NOTEBOOK] --oracle.host ORACLE_HOST [--oracle.port ORACLE_PORT] --oracle.username
                       ORACLE_USERNAME --oracle.password ORACLE_PASSWORD --oracle.database ORACLE_DATABASE [--oracle.table.list ORACLE_TABLE_LIST]
                       [--jdbctojdbc.output.output.mode {overwrite,append}] --postgres.host POSTGRES_HOST --postgres.port POSTGRES_PORT --postgres.username  POSTGRES_USERNAME --postgres.password  POSTGRES_PASSWORD--postgres.database POSTGRES_DATABASE --postgres.schema POSTGRES_SCHEMA [--jdbctojdbc.output.batch.size BATCH_SIZE]
                       [--jdbctojdbc.output.batch.size BATCH_SIZE]
                       [--max.parallelism MAX_PARALLELISM]
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
  --oracle.table.list ORACLE_TABLE_LIST
                        Oracle table list to migrate. Leave empty for migrating complete database else provide tables as "table1,table2"
  --postgres.host POSTGRES_HOST
                        Postgres host 
  --postgres.port POSTGRES_PORT
                        Postgres port (Default: 5432
  --postgres.username POSTGRES_USERNAME
                        Postgres username
  --postgres.password POSTGRES_PASSWORD
                          Postgres password
  --postgres.database POSTGRES_DATABASE
                          Postgres database
  --postgres.schema POSTGRES_SCHEMA
                          Postgres schema
  --jdbctojdbc.output.mode JDBCTOJDBC_OUTPUT_MODE
                            Postgres output write mode (Default: overwrite). Use append when schema already exists in postgres
  --jdbctojdbc.output.batch.size BATCH_SIZE
                              JDBC output batch size. Default set to 1000                   
  --max.parallelism MAX_PARALLELISM
                        Maximum number of tables that will migrated parallelly (Default: 5)
```

### Required JAR files

This notebook requires the Oracle and POSTGRES connector jars. Installation information is present in the notebook



