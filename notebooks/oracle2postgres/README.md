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
* `ORACLETABLE_LIST` : List of tables you want to migrate eg: ['table1','table2'] else provide empty list for migration whole database eg : []

#### POSTGRES Parameters
* `POSTGRES_HOST` : Postgres instance ip address
* `POSTGRES_PORT` : Postgres instance port
* `POSTGRES_USERNAME` : Postgres username
* `POSTGRES_PASSWORD` : Postgres password
* `POSTGRES_DATABASE` : name of database that you want to migrate to
* `POSTGRES_SCHEMA` : Postgres Schema
* `OUTPUT_MODE` : Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to overwrite)
* `BATCH_SIZE` : JDBC output batch size. Default set to 1000

### Required JAR files

This notebook requires the Oracle and POSTGRES connector jars. Installation information is present in the notebook



