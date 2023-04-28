import papermill as pm

nb_parameters = dict(
    not_parameterized=False,
    PROJECT = "yadavaja-sandbox",
    REGION = "us-west1", # eg: us-central1 (any valid GCP region)
    GCS_STAGING_LOCATION = "gs://tanyawarrier-temp", # eg: gs://my-staging-bucket/sub-folder
    SUBNET = "projects/yadavaja-sandbox/regions/us-west1/subnetworks/test-subnet1",
    MAX_PARALLELISM = 5, # max number of tables which will migrated parallelly 
    MYSQL_HOST = "10.203.209.3",
    MYSQL_PORT = "3306",
    MYSQL_USERNAME = "inttestuser",
    MYSQL_PASSWORD = "test123",
    MYSQL_DATABASE = "test",
    MYSQLTABLE_LIST = ["employee"], # leave list empty for migrating complete database else provide tables as ['table1','table2']
    MYSQL_OUTPUT_SPANNER_MODE = "overwrite", # one of overwrite|append (Use append when schema already exists in Spanner)
    SPANNER_INSTANCE = "dataproc-spark-test",
    SPANNER_DATABASE = "spark-ci-db",
    SPANNER_TABLE_PRIMARY_KEYS = {"employee":"empno"} # provide table & pk column which do not have PK in MYSQL {"table_name":"primary_key"}
)

pm.execute_notebook(
   'mysql2spanner/MySqlToSpanner_notebook.ipynb',
   'mysql2spanner/OUTPUT_MySqlToSpanner_notebook.ipynb',
   parameters = nb_parameters
)