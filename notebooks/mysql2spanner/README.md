## Jupyter Notebook Solution for migrating MySQL database to Cloud Spanner using Dataproc Templates

Notebook solution utilizing dataproc templates for migrating databases from MySQL to Cloud Spanner.  
It contains step by step process for migrating MySQL database to Cloud Spanner.  
The migration is done in 2 steps. Firstly data is exported from MySQL to GCS.  
Finally, data is read from Cloud Storage to Cloud Spanner.

Refer [Setup Vertex AI - PySpark](../generic_notebook/README.md) to setup new Jupyter notebook in vertexAI.
Once the setup is done navigate to `/notebooks/mysql2spanner` folder and open
[MySqlToSpanner_notebook](./MySqlToSpanner_notebook.ipynb).

### Overview

This notebook is built on top of:

* [Vertex AI Jupyter Notebook](https://cloud.google.com/vertex-ai/docs/tutorials/jupyter-notebooks)
* [Google Cloud's Dataproc Serverless](https://cloud.google.com/dataproc-serverless/)
* Dataproc Templates which are maintained in this github project.

### Key Benefits

1) Automatically discovers all the MySQL tables.
2) Can automatically generates table schema in Cloud Spanner, corresponding to each table.
3) Divides the migration into multiple batches and automatically computes metadata.
4) Parallely migrates mutiple MySQL tables to Cloud Spanner.
5) Automatically partitioned reading of large tables (1 Million+ rows) and parallely import data, speeding up the data movemment.
6) Simple, easy to use and customizable.

### Requirements

Below configurations are required before proceeding further.

#### Common Parameters

* `PROJECT` : GCP project-id
* `REGION` : GCP region
* `GCS_STAGING_LOCATION` : Cloud Storage staging location to be used for this notebook to store artifacts (eg: gs://bucket-name)
* `SUBNET` : VPC subnet
* `JARS` : list of jars. For this notebook mysql connector and avro jar is required in addition with the dataproc template jars
* `MAX_PARALLELISM` : Parameter for number of jobs to run in parallel default value is 2

#### MYSQL to Cloud Storage Parameters

* `MYSQL_HOST` : MYSQL instance ip address
* `MYSQL_PORT` : MySQL instance port
* `MYSQL_USERNAME` : MYSQL username
* `MYSQL_PASSWORD` : MYSQL password
* `MYSQL_DATABASE` : name of database that you want to migrate
* `MYSQL_TABLE_LIST` : list of tables you want to migrate eg: ['table1','table2'] else provide empty list for migration whole database eg : [] 
* `MYSQL_OUTPUT_GCS_LOCATION` : Cloud Storage location where mysql output will be writtes eg :"gs://bucket/[folder]"
* `MYSQL_OUTPUT_GCS_MODE` : output mode for MYSQL data one of (overwrite|append)
* `MYSQL_OUTPUT_GCS_FORMAT` : output file formate for MYSQL data one of (avro|parquet|orc)

#### Cloud Storage to Cloud Spanner Parameters

* `SPANNER_INSTANCE` : Cloud Spanner instance name
* `SPANNER_DATABASE` : Cloud Spanner database name
* `SPANNER_TABLE_PRIMARY_KEYS` : provide dictionary of format {"table_name":"primary_key"} for tables which do not have primary key in MYSQL

### Run programmatically with parameterize script

Alternatively to running the notebook manually, we developed a "parameterize" script, using the papermill lib, to allow running notebooks programmatically from a Python script, with parameters.  

**Example submission:**

```shell
export GCP_PROJECT=<project>
export REGION=<region>
export GCS_STAGING_LOCATION=gs://<bucket-name>
export SUBNET=<subnet>

python run_notebook.py --script=MYSQLTOSPANNER \
                        --mysql.host="10.x.x.x" \
                        --mysql.port="3306" \
                        --mysql.username="user" \
                        --mysql.password="password" \
                        --mysql.database="db" \
                        --mysql.table.list="employee" \
                        --spanner.instance="spark-instance" \
                        --spanner.database="spark-db" \
                        --spanner.table.primary.keys="{\"employee\":\"empno\"}"
```

**Parameters:**

```
python run_notebook.py --script=MYSQLTOSPANNER --help

usage: run_notebook.py [-h]
        --mysql.host MYSQL_HOST
        [--mysql.port MYSQL_PORT]
        --mysql.username MYSQL_USERNAME
        --mysql.password MYSQL_PASSWORD
        --mysql.database MYSQL_DATABASE
        [--mysql.table.list MYSQL_TABLE_LIST]
        [--mysql.output.spanner.mode {overwrite,append}]
        --spanner.instance SPANNER_INSTANCE
        --spanner.database SPANNER_DATABASE
        --spanner.table.primary.keys SPANNER_TABLE_PRIMARY_KEYS
        [--max.parallelism MAX_PARALLELISM]
        [--output.notebook OUTPUT.NOTEBOOK]
        [--log_level {NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL}]

optional arguments:
    -h, --help            
        show this help message and exit
    --mysql.host MYSQL_HOST
        MySQL host or IP address
    --mysql.port MYSQL_PORT
        MySQL port (Default: 3306)
    --mysql.username MYSQL_USERNAME
        MySQL username
    --mysql.password MYSQL_PASSWORD
        MySQL password
    --mysql.database MYSQL_DATABASE
        MySQL database name
    --mysql.table.list MYSQL_TABLE_LIST
        MySQL table list to migrate. Leave empty for migrating complete database else provide tables as "table1,table2"
    --mysql.output.spanner.mode {overwrite,append}
        Spanner output write mode (Default: overwrite). Use append when schema already exists in Spanner
    --spanner.instance SPANNER_INSTANCE
        Spanner instance name
    --spanner.database SPANNER_DATABASE
        Spanner database name
    --spanner.table.primary.keys SPANNER_TABLE_PRIMARY_KEYS
        Provide table & PK column which do not have PK in MySQL table {"table_name":"primary_key"}
    --max.parallelism MAX_PARALLELISM
        Maximum number of tables that will migrated parallelly (Default: 5)
    --output.notebook OUTPUT.NOTEBOOK
        Path to save executed notebook (Default: None). If not provided, no notebook is saved
    --log_level {NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL}
        Papermill's Execute Notebook log level (Default: INFO)
```

### Required JAR files

This notebook requires the MYSQL connector jar. Installation information is present in the notebook

### Limitations

* Does not work with Cloud Spanner's Postgresql Interface
