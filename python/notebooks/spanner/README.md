## Jupyter Notebook Solution for migrating MySQL database to CLoud Spanner using Dataproc Templates

Template for reading tables from HIVE Database and writing to BigQuery Dataset.

Refer [Setup Vertex AI - PySpark](./../README.md) to setup new Jupyter notebook in vertexAI. Once the setup is done navigate to 
[dataproc-templates/python/notebooks/spanner](https://github.com/GoogleCloudPlatform/dataproc-templates/blob/mysql-to-spanner-nb/python/notebooks/spanner/) folder and open [mysql_to_spanner.ipynb](https://github.com/GoogleCloudPlatform/dataproc-templates/blob/mysql-to-spanner-nb/python/notebooks/spanner/mysql-to-spanner.ipynb) notebook.

### Overview

[mysql_to_spanner.ipynb](./mysql_to_spanner.ipynb) dataproc template is built on top of [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks) and [Google Cloud's Dataproc](https://cloud.google.com/dataproc/) tech stack provided by GCP.


### Requirements

Below configurations are required before proceeding further.

* `REGION`: GCP Region  to unload HIVE tables in BQ.
* `GCS_STAGING_LOCATION`: GCS bucket to store artefacts.
* `SUBNET`: VPC Subnet
* `INPUT_HIVE_DATABASE`: Hive database for input tables
* `INPUT_HIVE_TABLES`: Comma seperated hive tablenames to move, in case you want to move all the tables put "*"
* `OUTPUT_BIGQUERY_DATASET`: BigQuery dataset for the output tables
* `TEMP_BUCKET`: Temporary GCS bucket to store intermediate files.
* `HIVE_METASTORE`: Hive metastore URI
* `MAX_PARALLELISM`: Number of parallel Dataproc Jobs to run (default=10)

### Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

### Procedure to load BQ tables from HIVE:
Step by Step instructions ar given before each cell in the python notebook.

* Add user configuration in Step 1
* Run all the cells from Menu->Run->Run All Cells
* Get the status of Dataproc Jobs from VertexAI UI using the link printed after running Step 11
* Detailed logs can be seen from [Dataproc Batch UI](https://console.cloud.google.com/dataproc/batches?_ga=2.45339748.1795356115.1659430333-470209831.1657040299)
  * Dataproc Job naming convention: "hive2bq-"+HIVE_TABLE+"-"+ CURRENT_DATETIME

