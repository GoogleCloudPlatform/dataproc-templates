## Dataproc Template to Migrate Hive tables to Bigquery using Jupyter Notebooks

This Vertex AI notebook leverages the [HiveToBQ template](/python/dataproc_templates/hive#hive-to-bigquery) 
and performs an orchestrated submission of several PySpark jobs using Dataproc Serverless to load multiple 
tables simultaneously from Hive database to Bigquery dataset.

Refer [Setup Vertex AI - PySpark](../generic_notebook/README.md) to setup new Jupyter notebook in vertexAI. 
Once the setup is done navigate to `/notebooks/hive2bq` folder and open 
[HiveToBigquery_notebook](HiveToBigquery_notebook.ipynb) notebook.

### Overview
This notebook is built on top of:
* [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks) 
* [Google Cloud's Dataproc Serverless](https://cloud.google.com/dataproc-serverless/)
* Dataproc Templates which are maintained in this github project.

### Key Benefits
1) Automatically discovers all the Hive tables.
2) Can automatically extract all the table DDLs using [HIVEDDLEXTRACTOR](/python/dataproc_templates/hive/util#hive-ddl-extractor)
3) Can create partitioned and clustered tables in Bigquery based on HIVE partitioning and clusering using [BQ Translation API](https://cloud.google.com/bigquery/docs/migration-intro)
4) Divides the migration into multiple batches and automatically computes metadata.
5) Parallely migrates mutiple Hive tables to BigQuery.
6) Simple, easy to use and customizable.

### Requirements

Below configurations are used to execute these notebooks.

* `REGION`: GCP Region  to unload Hive tables in BQ.
* `GCS_STAGING_LOCATION`: Cloud Storage bucket to store artefacts. (gs://bucket-name)
* `SUBNET`: VPC Subnet
* `INPUT_HIVE_DATABASE`: Hive database for input tables
* `INPUT_HIVE_TABLES`: Comma seperated Hive tablenames to move, in case you want to move all the tables put "*"
* `OUTPUT_BIGQUERY_DATASET`: BigQuery dataset for the output tables
* `TEMP_BUCKET`: Temporary Cloud Storage bucket to store intermediate files.
* `HIVE_METASTORE`: Hive metastore URI
* `MAX_PARALLELISM`: Number of parallel Dataproc Jobs to run (default=10)
* `BQ_DATASET_REGION`: BQ Dataset Region

### Run programmatically with parameterize script

Alternatively to running the notebook manually, we developed a "parameterize" script, using the papermill lib, to allow running notebooks programmatically from a Python script, with parameters.

**Example submission:**

```shell
export GCP_PROJECT=<project>
export REGION=<region>
export GCS_STAGING_LOCATION=gs://<bucket-name>
export SUBNET=<subnet>
export SERVICE_ACCOUNT=<service-account>

python run_notebook.py --script=HIVETOBIGQUERY \
                        --hive.metastore=thrift://10.0.0.33:9083 \
                        --input.hive.database="defaultdb" \
                        --input.hive.table="employee" \
                        --output.bigquery.dataset="hive_to_bq_ds" \
                        --temp.bucket="mybucket-temp" \
                        --hive.output.mode="overwrite"
```

**Parameters:**

```
python run_notebook.py --script=HIVETOBIGQUERY --help
usage: run_notebook.py [-h] [--output.notebook OUTPUT.NOTEBOOK] --hive.metastore HIVE_METASTORE --input.hive.database INPUT_HIVE_DATABASE [--input.hive.tables INPUT_HIVE_TABLES] --output.bigquery.dataset
                       OUTPUT_BIGQUERY_DATASET --temp.bucket TEMP_BUCKET [--hive.output.mode {overwrite,append}] [--max.parallelism MAX_PARALLELISM]

optional arguments:
  -h, --help            show this help message and exit
  --output.notebook OUTPUT.NOTEBOOK
                        Path to save executed notebook (Default: None). If not provided, no notebook is saved
  --hive.metastore HIVE_METASTORE
                        Hive metastore URI
  --input.hive.database INPUT_HIVE_DATABASE
                        Hive database name
  --input.hive.tables INPUT_HIVE_TABLES
                        Comma separated list of Hive tables to be migrated "/table1,table2,.../" (Default: *)
  --output.bigquery.dataset OUTPUT_BIGQUERY_DATASET
                        BigQuery dataset name
  --temp.bucket TEMP_BUCKET
                        Cloud Storage bucket name for temporary staging
  --hive.output.mode {overwrite,append}
                        Hive output mode (Default: overwrite)
  --max.parallelism MAX_PARALLELISM
                        Maximum number of tables that will migrated parallelly (Default: 5)
```

### Required JAR files

This template requires the
[Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example)
to be available in the Dataproc cluster.

### Procedure to load BQ tables from Hive:

Step by Step instructions ar given before each cell in the python notebook.

* Add user configuration in Step 1
* Run all the cells from Menu->Run->Run All Cells
* Skip running steps 8-16 to create all HIVE tables(partitioned & non-partitioned) as non partitioned tables in Bigquery
* Get the status of Dataproc Jobs from VertexAI UI using the link printed after running Step 11
* Detailed logs can be seen from [Dataproc Batch UI](https://console.cloud.google.com/dataproc/batches)
  * Dataproc Job naming convention: "hive2bq-"+HIVE-TABLE+"-"+ CURRENT_DATETIME

### Parallel Jobs

Once the notebook is triggered, you can visualize parallel jobs by either using the link generated after 
Step 11 or by finding your job in [VertexAI Pipelines](https://console.cloud.google.com/vertex-ai/pipelines/).

![workbench](images/HiveToBQ_Flow.png)

### Audit Table

The template stores audit data for each load in CSV format in Cloud Storage bucket provided.

In order to view the data create an external table pointing to the Cloud Storage bucket as below.

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

* The current version does not support incremental load.
* User has to implement Kerberos authentication themselves if needed.
