## Jupyter Notebook Solution for migrating Oracle database to BigQuery using Dataproc Templates

Notebook solution utilizing dataproc templates for migrating databases from Oracle to BigQuery. Notebook contains step by step process for migrating Oracle database tables to BigQuery.

Refer [Setup Vertex AI - PySpark](./../generic_notebook/README.md) to setup new Jupyter notebook in vertexAI. Once the setup is done navigate to 
[dataproc-templates/python/notebooks/oracle2bq](https://github.com/GoogleCloudPlatform/dataproc-templates/tree/oracle2bq-notebook/notebooks/oracle2bq) folder and open [OracleToBigQuery_vertex_pipeline_pyspark.ipynb](https://github.com/GoogleCloudPlatform/dataproc-templates/blob/oracle2bq-notebook/notebooks/oracle2bq/OracleToBigQuery_notebook.ipynb) notebook.

### Overview

[OracleToBigQuery_vertex_pipeline_pyspark.ipynb](https://github.com/GoogleCloudPlatform/dataproc-templates/blob/oracle2bq-notebook/notebooks/oracle2bq/OracleToBigQuery_notebook.ipynb) notebook solution is built on top of [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks) and [Google Cloud's Dataproc](https://cloud.google.com/dataproc/) tech stack provided by GCP.

### Key Benifits

* Automatically Generate list of tables from metadata. Alternatively, user should be able to supply list of tables.
* Identify current primary key column name, and partitioned read properties.
* Automatically uses partition reads if exceeds threshold.
* Divides migration into batches and parallely migrates multiple tables.
* Notebook allow you to choose modes i.e. appending data or overwrite.
* Bigquery load automatically created table if table does not exists.

### Requirements

Below configurations are required before proceeding further.
#### Common Parameters

* `PROJECT` : GCP project-id
* `REGION` : GCP region
* `GCS_STAGING_LOCATION` : GCS staging location to be used for this notebook to store artifacts
* `SUBNET` : VPC subnet
* `JARS` : list of jars. For this notebook oracle driver and bigquery connector with the dataproc template jars
* `MAX_PARALLELISM` : Parameter for number of jobs to run in parallel default value is 2

#### Oracle Parameters
* `ORACLE_HOST` : ORACLE instance ip address
* `ORACLE_PORT` : ORACLE instance port
* `ORACLE_USERNAME` : ORACLE username
* `ORACLE_PASSWORD` : ORACLE password
* `ORACLE_DATABASE` : name of database that you want to migrate
* `ORACLETABLE_LIST` : list of tables you want to migrate eg: ['table1','table2'] else provide empty list for migration whole database eg : [] 

#### BigQuery Parameters
* `BIGQUERY_DATASET` : BigQuery Target Dataset
* `BIGQUERY_MODE` : Mode of operation at target append/overwrite

### Required JAR files

This notebook requires the Oracle driver and BigQuery Connector jar. Installation information is present in the notebook



