## Jupyter Notebook Solution for migrating Oracle database to Cloud Spanner using Dataproc Templates

Notebook solution utilizing dataproc templates for migrating databases from Oracle to Cloud Spanner.
It contains step by step process for migrating Oracle database to Cloud Spanner.

Refer [Setup Vertex AI - PySpark](../generic_notebook/README.md) to setup new Jupyter notebook in vertexAI.
Once the setup is done navigate to `/notebooks/oracle2spanner` folder and open
[OracleToSpanner_notebook](./OracleToSpanner_notebook.ipynb).

### Overview

This notebook is built on top of:
* [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks)
* [Google Cloud's Dataproc Serverless](https://cloud.google.com/dataproc-serverless/)
* Dataproc Templates which are maintained in this github project.

### Key Benefits
1) Automatically discovers all the Oracle tables.
2) Can automatically generates table schema in Cloud Spanner, corresponding to each table.
3) Divides the migration into multiple batches and automatically computes metadata.
4) Parallely migrates mutiple Oracle tables to Cloud Spanner.
5) Simple, easy to use and customizable.

### Requirements

Below configurations are required before proceeding further.
#### Common Parameters

* `PROJECT` : GCP project-id
* `REGION` : GCP region
* `GCS_STAGING_LOCATION` : Cloud Storage staging location to be used for this notebook to store artifacts
* `SUBNET` : VPC subnet
* `JARS` : List of jars. For this notebook Oracle connector jar is required in addition with the Dataproc template jars
* `MAX_PARALLELISM` : Parameter for number of jobs to run in parallel default value is 5

#### Oracle to Cloud Spanner Parameters
* `ORACLE_HOST` : Oracle instance ip address
* `ORACLE_PORT` : Oracle instance port
* `ORACLE_USERNAME` : Oracle username
* `ORACLE_PASSWORD` : Oracle password
* `ORACLE_DATABASE` : Name of database/service for Oracle connection
* `ORACLE_TABLE_LIST` : List of tables you want to migrate eg: ['table1','table2'] else provide an empty list for migration whole database eg : []
* `SPANNER_OUTPUT_MODE`: <Append | Overwrite>
* `SPANNER_INSTANCE` : Cloud Spanner instance name
* `SPANNER_DATABASE` : Cloud Spanner database name
* `SPANNER_TABLE_PRIMARY_KEYS` : Provide dictionary of format {"table_name":"primary_key"} for tables which do not have primary key in Oracle


### Required JAR files

This notebook requires the Oracle connector jar. Installation information is present in the notebook


### Limitations:

* Does not work with Cloud Spanner's Postgresql Interface

