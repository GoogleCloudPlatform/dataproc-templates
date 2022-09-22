## Dataproc Template to Migrate Hive tables to Bigquery using Jupyter Notebooks

This Vertex AI notebook leverages the [HiveToBQ template](../../dataproc_templates/hive/hive_to_bigquery.py) and performs an orchestrated submission of several PySpark jobs using Dataproc Serverless to load multiple tables simultaneously from Hive database to Bigquery dataset.

Refer [Setup Vertex AI - PySpark](./../README.md) to setup new Jupyter notebook in vertexAI. Once the setup is done navigate to 
[dataproc-templates/python/notebooks/hive](../hive) folder and open [HivetoBigquery_vertex_pipeline_pyspark](./HiveToBigquery_vertex_pipeline_pyspark.ipynb) notebook.

### Overview

[HiveToBQ- PySpark Notebook](./HiveToBigquery_vertex_pipeline_pyspark.ipynb) dataproc template is built on top of [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks) and [Google Cloud's Dataproc](https://cloud.google.com/dataproc/) tech stack provided by GCP.


### Requirements

Below configurations are required before proceeding further.

* `REGION`: GCP Region  to unload Hive tables in BQ.
* `GCS_STAGING_LOCATION`: GCS bucket to store artefacts.
* `SUBNET`: VPC Subnet
* `INPUT_HIVE_DATABASE`: Hive database for input tables
* `INPUT_HIVE_TABLES`: Comma seperated Hive tablenames to move, in case you want to move all the tables put "*"
* `OUTPUT_BIGQUERY_DATASET`: BigQuery dataset for the output tables
* `TEMP_BUCKET`: Temporary GCS bucket to store intermediate files.
* `HIVE_METASTORE`: Hive metastore URI
* `MAX_PARALLELISM`: Number of parallel Dataproc Jobs to run (default=10)

### Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

### Procedure to load BQ tables from Hive:
Step by Step instructions ar given before each cell in the python notebook.

* Add user configuration in Step 1
* Run all the cells from Menu->Run->Run All Cells
* Get the status of Dataproc Jobs from VertexAI UI using the link printed after running Step 11
* Detailed logs can be seen from [Dataproc Batch UI](https://console.cloud.google.com/dataproc/batches?_ga=2.45339748.1795356115.1659430333-470209831.1657040299)
  * Dataproc Job naming convention: "hive2bq-"+HIVE_TABLE+"-"+ CURRENT_DATETIME

### Parallel Jobs
Once the notebook is triggered, you can visualize parallel jobs by either using the link generated after Step 11 or by finding your job in [VertexAI Pipelines](https://console.cloud.google.com/vertex-ai/pipelines/).

![workbench](./../images/HiveToBQ_Flow.png)


### Audit Table

The template stores audit data for each load in CSV format in GCS bucket provided.

In order to view the data create an external table pointing to the GCS bucket as below.


```
 CREATE EXTERNAL TABLE `<project-id>.<dataset-name>.hive_bq_audit`
(
  Source_DB_Name STRING,
  Source_Table_Set STRING,
  Target_DB_Name STRING,
  Target_Table_Set STRING,
  Job_Start_Time STRING,
  Job_End_Time STRING,
  Job_Status STRING
)
OPTIONS(
  format="CSV",
  uris=["gs://<bucket-name>/audit/*"]
);
```

### Limitations:

* The current version does not currently migrate hive table partitions to Bigquery.
* The current version does not support incremetal load.
* User has to implement Kerberos authentication themselves if needed.
