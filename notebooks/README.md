## Dataproc Templates (Notebooks)

* [HiveToBigQuery](/notebooks/hive2bq#dataproc-template-to-migrate-hive-tables-to-bigquery-using-jupyter-notebooks) (blogpost [link](https://medium.com/google-cloud/hive-to-bigquery-move-data-efficiently-using-gcp-dataproc-serverless-ee30d35aaf03))
* [MsSqlToBigQuery](/notebooks/mssql2bq#jupyter-notebook-solution-for-migrating-mssql-sql-server-to-bigquery-dwh-using-dataproc-templates) (blogpost [link](https://medium.com/google-cloud/mssql-to-bigquery-migrate-efficiently-using-vertex-ai-notebook-and-gcp-dataproc-serverless-98358943568a))
* [MySQLToSpanner](/notebooks/mysql2spanner#jupyter-notebook-solution-for-migrating-mysql-database-to-cloud-spanner-using-dataproc-templates) (blogpost [link](https://medium.com/google-cloud/mysql-to-cloud-spanner-migrate-mysql-database-to-cloud-spanner-using-vertex-ai-notebooks-and-gcp-ad7d2ed8a317))
* [OracleToBigQuery](/notebooks/oracle2bq#jupyter-notebook-solution-for-migrating-oracle-database-to-bigquery-using-dataproc-templates)
* [OracleToPostgres](/notebooks/oracle2postgres#jupyter-notebook-solution-for-migrating-oracle-database-to-postgres-using-dataproc-templates)
(blogpost [Link](https://medium.com/google-cloud/oracle-to-postgresql-migrate-oracle-to-postgresql-using-vertex-ai-notebooks-and-gcp-dataproc-9924f3f5c51f))
* [OracleToSpanner](/notebooks/oracle2spanner#jupyter-notebook-solution-for-migrating-oracle-database-to-cloud-spanner-using-dataproc-templates) (blogpost [Link](https://medium.com/@surjitsh/oracle-to-cloud-spanner-migrate-oracle-database-to-cloud-spanner-using-vertex-ai-notebooks-and-gcp-49152ce7f4e8))
* [SQLServerToPostgres](/notebooks/mssql2postgresql#jupyter-notebook-solution-for-migrating-mssql-sql-server-to-postgres-database-using-dataproc-templates)

## Getting Started

* [Vertex AI Pipelines - PySpark](generic_notebook/vertex_pipeline_pyspark.ipynb)

Notebooks in this folder demonstrate how to run **Dataproc Templates** from Jupyter Notebooks using Vertex AI. 

### Overview

Recently, Google made Serverless Spark even more powerful, by enabling serverless interactive development through [Dataproc Sessions](https://cloud.google.com/blog/products/data-analytics/making-serverless-spark-even-more-powerful) in Jupyter notebooks, natively integrated with [Vertex AI Workbench](https://cloud.google.com/vertex-ai-workbench).  

Additionally, a data scientist can automate a Dataproc Template execution with [Vertex AI Pipelines](https://cloud.google.com/vertex-ai/docs/pipelines/introduction) and [Serverless Spark Kubeflow components](https://cloud.google.com/blog/topics/developers-practitioners/announcing-serverless-spark-components-vertex-ai-pipelines).  

### Deploying Dataproc Templates to Vertex AI

The best way to get started is to clone the Dataproc Templates repository to your Jupyter environment in Vertex AI, and run the notebook.  

1) Enable Compute Engine API, Dataproc API, Vertex-AI API and Vertex Notebooks API in your GCP project.
2) Create a [User-Managed](https://cloud.google.com/vertex-ai/docs/workbench/user-managed/introduction) 
Notebook in Vertex AI Workbench

   ![workbench](generic_notebook/images/create_notebook.png)

   In this example, a User-Managed notebook is created using the Compute Engine default service account.

3) Open the created notebook, clone the [Dataproc Templates](https://github.com/GoogleCloudPlatform/dataproc-templates) 
GitHub repository and run the desired notebook located in the */notebooks* folder

   ![clone](generic_notebook/images/clone_repository.png)

### Run notebooks programmatically

Alternatively to running the notebook manually, we developed a "parameterize" script, using the papermill lib, to allow running notebooks programmatically from a Python script, with parameters.  
You can see each specific parameters in each notebook type README.  

It is currently available for the following notebooks:

* [HiveToBigQuery](/notebooks/hive2bq#dataproc-template-to-migrate-hive-tables-to-bigquery-using-jupyter-notebooks)
* [MySQLToSpanner](/notebooks/mysql2spanner#jupyter-notebook-solution-for-migrating-mysql-database-to-cloud-spanner-using-dataproc-templates)
* [OracleToPostgres](/notebooks/oracle2postgres#jupyter-notebook-solution-for-migrating-oracle-database-to-postgres-using-dataproc-templates)
* [OracleToBigQuery](/notebooks/oracle2bq#jupyter-notebook-solution-for-migrating-oracle-database-to-bigquery-using-dataproc-templates)
* [PostgreSQLToBigQuery](/notebooks/postgresql2bq#jupyter-notebook-solution-for-migrating-postgresql-to-bigquery-dwh-using-dataproc-templates)

USAGE:  

```shell
export GCP_PROJECT=<project>
export REGION=<region>
export GCS_STAGING_LOCATION=<gs://bucket-name>
export SUBNET=<subnet>

python run_notebook.py --script=<NOTEBOOK_NAME> \
   --log_level=<LOG_LEVEL> \
   --notebook.paramter1="<>" \
   --notebook.paramter2="<>"
```

### Deploying Notebook Directly to Colab
If you directly open notebook to colab environment then it will pull out notebook only. Our notebooks requires certain packages which comes along with repository such as [util](https://github.com/GoogleCloudPlatform/dataproc-templates/tree/main/notebooks/util). Please follow below steps for each packages which requires in imported notebook.
```jupyter
!git clone https://github.com/GoogleCloudPlatform/dataproc-templates.git
!mv /content/dataproc-templates/notebooks/util /content/
!mv /content/dataproc-templates/java/ /content/
```
