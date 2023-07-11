## Jupyter Notebook Solution for migrating MSSQL (SQL Server) to POSTGRES Database using Dataproc Templates

Notebook solution utilizing dataproc templates for migrating databases from MSSQL to POSTGRES. 
Notebook contains step by step process for a downtime based migration.

Refer [Setup Vertex AI - PySpark](../generic_notebook/README.md) to setup new Jupyter notebook in vertexAI.
Once the setup is done navigate to `/notebooks/mssql2postgresql` folder and open
[mssql-to-postgres-notebook](./mssql-to-postgres-notebook.ipynb).

### Overview

This notebook is built on top of:
* [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks)
* [Google Cloud's Dataproc Serverless](https://cloud.google.com/dataproc-serverless/)
* Dataproc Templates which are maintained in this github project.

### Key Benefits
1) Automatically discovers all the SQL Server tables.
2) Can automatically generates table schema in Postgresql, corresponding to each table.
3) Divides the migration into multiple batches and automatically computes metadata.
4) Parallely migrates mutiple SQL Server tables to Postgresql.
5) Simple, easy to use and customizable.

### Requirements

Below configurations are required before proceeding further.
#### Common Parameters

* `PROJECT` : GCP project-id
* `REGION` : GCP region
* `GCS_STAGING_LOCATION` : Cloud Storage staging location to be used for this notebook to store artifacts
* `SUBNET` : VPC subnet
* `JARS` : list of jars. For this notebook mssql and postgres connector jars are required in addition with the dataproc template jars
* `MAX_PARALLELISM` : Parameter for number of jobs to run in parallel default value is 2

#### MSSQL Parameters
* `MSSQL_HOST` : MSSQL instance ip address
* `MSSQL_PORT` : MSSQL instance port
* `MSSQL_USERNAME` : MSSQL username
* `MSSQL_PASSWORD` : MSSQL password
* `MSSQL_DATABASE` : name of database that you want to migrate
* `MSSQLTABLE_LIST` : list of tables you want to migrate eg: 'table1','table2' else keep empty for migration whole database
* `NUMBER_OF_PARTITIONS` : The maximum number of partitions that can be used for parallelism in table reading and writing. Same value will be used for both input and output jdbc connection. Default set to 10

#### POSTGRES Parameters
* `POSTGRES_HOST` : MSSQL instance ip address
* `POSTGRES_PORT` : MSSQL instance port
* `POSTGRES_USERNAME` : MSSQL username
* `POSTGRES_PASSWORD` : MSSQL password
* `POSTGRES_DATABASE` : name of database that you want to migrate to
* `OUTPUT_MODE` : Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to overwrite)
* `BATCH_SIZE` : JDBC output batch size. Default set to 1000

### Required JAR files

This notebook requires the MSSQL and POSTGRES connector jars. Installation information is present in the notebook



