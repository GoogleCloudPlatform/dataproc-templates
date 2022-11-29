## Dataproc Templates (Notebooks)

* [HiveToBigQuery](/notebooks/hive2bq) (blogpost [link](https://medium.com/google-cloud/hive-to-bigquery-move-data-efficiently-using-gcp-dataproc-serverless-ee30d35aaf03))
* [SQLServerToPostgres](/notebooks/mssql2postgresql)
* [MySQLToSpanner](/notebooks/mysql2spanner) (blogpost [link](https://medium.com/google-cloud/mysql-to-cloud-spanner-migrate-mysql-database-to-cloud-spanner-using-vertex-ai-notebooks-and-gcp-ad7d2ed8a317))
* [OracleToBigQuery](/notebooks/oracle2bq)
* [OracleToSpanner](/notebooks/oracle2spanner) (blogpost [Link](https://medium.com/@surjitsh/oracle-to-cloud-spanner-migrate-oracle-database-to-cloud-spanner-using-vertex-ai-notebooks-and-gcp-49152ce7f4e8))
* [MsSqlToBigQuery](/notebooks/mssql2bq)

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