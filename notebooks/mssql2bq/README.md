## Jupyter Notebook Solution for migrating MSSQL (SQL Server) to BigQuery DWH using Dataproc Templates


This notebook solution utilizes dataproc templates for migrating databases from Microsoft SQL Server to BIGQUERY. 

The notebook contains step by step process for a downtime based migration.

Refer [Setup Vertex AI - PySpark](../generic_notebook/README.md) to setup new Jupyter notebook in vertexAI.
Once the setup is done navigate to `/notebooks/mssql2bq` folder and open
[mssql-to-bigquery-notebook](./mssql-to-bigquery-notebook.ipynb).

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
* `JARS` : list of jars. For this notebook mssql and Spark Bigquery connector jars are required in addition to the dataproc template jars
* `MAX_PARALLELISM` : Parameter for number of jobs to run in parallel default value is 5

#### SQL Server Parameters
* `SQL_SERVER_HOST` : SQL Server instance ip address
* `SQL_SERVER_PORT` : SQL Server instance port
* `SQL_SERVER_USERNAME` : SQL Server username
* `SQL_SERVER_PASSWORD` : SQL Server password
* `SQL_SERVER_DATABASE` : Name of database that you want to migrate
* `SQL_SERVER_TABLE_LIST` : List of tables you want to migrate eg ['schema.table1','schema.table2'] else provide an empty list for migration of specific schemas or the whole database. Example: [].
* `SQL_SERVER_SCHEMA_LIST` : List of schemas. Use this if you'ld like to migrate all tables associated with specific schemas eg. ['schema1','schema2']. Otherwise, leave this parameter empty. Example: []. This comes in handy when don't want to provide names of all the tables separately but would rather prefer migrating all tables from a schema.

#### BigQuery Parameters

* `BIGQUERY_DATASET` : BigQuery Target Dataset
* `BIGQUERY_MODE` : Mode of operation at target append/overwrite


### Required JAR files

This notebook requires the MSSQL and POSTGRES connector jars. Installation information is present in the notebook



