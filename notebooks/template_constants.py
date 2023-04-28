#common
PROJECT = "project.id"
REGION = "region" # eg: us-central1 (any valid GCP region)
GCS_STAGING_LOCATION = "gcs.staging.location" # eg: gs://my-staging-bucket/sub-folder
SUBNET = "subnet"
MAX_PARALLELISM = "max.parallelism" # max number of tables which will migrated parallelly 

# MySQL Parameters
MYSQL_HOST = "mysql.host"
MYSQL_PORT = "mysql.port"
MYSQL_USERNAME = "mysql.username"
MYSQL_PASSWORD = "mysql.password"
MYSQL_DATABASE = "mysql.database"
MYSQLTABLE_LIST = "mysql.list" # leave list empty for migrating complete database else provide tables as ['table1','table2']
MYSQL_OUTPUT_SPANNER_MODE = "mysql.output.spanner.mode" # one of overwrite|append (Use append when schema already exists in Spanner)
SPANNER_INSTANCE = "spanner.instance"
SPANNER_DATABASE = "spanner.database"
SPANNER_TABLE_PRIMARY_KEYS = "spanner.table.primary.keys" # provide table & pk column which do not have PK in MYSQL {"table_name":"primary_key"}