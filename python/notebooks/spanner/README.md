## Jupyter Notebook Solution for migrating MySQL database to Cloud Spanner using Dataproc Templates

Notebook solution utilizing dataproc templates for migrating databases from MySQL to Cloud Spanner. Notebook contains step by step process for migrating MySQL database to Cloud Spanner. The migration is done in 2 steps. Firstly data is exported from MySQL to GCS. Finally, data is read from GCS to Cloud Spanner.

Refer [Setup Vertex AI - PySpark](./../README.md) to setup new Jupyter notebook in vertexAI. Once the setup is done navigate to 
[dataproc-templates/python/notebooks/spanner](https://github.com/GoogleCloudPlatform/dataproc-templates/blob/mysql-to-spanner-nb/python/notebooks/spanner/) folder and open [MySqlToSpanner_vertex_pipeline_pyspark.ipynb](https://github.com/GoogleCloudPlatform/dataproc-templates/blob/mysql-to-spanner-nb/python/notebooks/spanner/mysql-to-spanner.ipynb) notebook.

### Overview

[MySqlToSpanner_vertex_pipeline_pyspark.ipynb](https://github.com/GoogleCloudPlatform/dataproc-templates/blob/mysql-to-spanner-nb/python/notebooks/spanner/mysql-to-spanner.ipynb) notebook solution is built on top of [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks) and [Google Cloud's Dataproc](https://cloud.google.com/dataproc/) tech stack provided by GCP.


### Requirements

Below configurations are required before proceeding further.
#### Common Parameters

* `PROJECT` : GCP project-id
* `REGION` : GCP region
* `GCS_STAGING_LOCATION` : GCS staging location to be used for this notebook to store artifacts
* `SUBNET` : VPC subnet
* `JARS` : list of jars. For this notebook mysql connector and avro jar is required in addition with the dataproc template jars
* `MAX_PARALLELISM` : Parameter for number of jobs to run in parallel default value is 2

#### MYSQL to GCS Parameters
* `MYSQL_HOST` : MYSQL instance ip address
* `MYSQL_PORT` : MySQL instance port
* `MYSQL_USERNAME` : MYSQL username
* `MYSQL_PASSWORD` : MYSQL password
* `MYSQL_DATABASE` : name of database that you want to migrate
* `MYSQLTABLE_LIST` : list of tables you want to migrate eg: ['table1','table2'] else provide empty list for migration whole database eg : [] 
* `MYSQL_OUTPUT_GCS_LOCATION` : gcs location where mysql output will be writtes eg :"gs://bucket/[folder]"
* `MYSQL_OUTPUT_GCS_MODE` : output mode for MYSQL data one of (overwrite|append)
* `MYSQL_OUTPUT_GCS_FORMAT` : output file formate for MYSQL data one of (avro|parquet|orc)

#### GCS to Cloud Spanner Parameters
* `SPANNER_INSTANCE` : cloud spanner instance name
* `SPANNER_DATABASE` : cloud spanner database name
* `SPANNER_TABLE_PRIMARY_KEYS` : provide dictionary of format {"table_name":"primary_key"} for tables which do not have primary key in MYSQL

### Required JAR files

This notebook requires the MYSQL connector jar. Installation information is present in the notebook



