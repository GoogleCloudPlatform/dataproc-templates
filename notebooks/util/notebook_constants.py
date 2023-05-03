# Common
GCP_PROJECT = "GCP_PROJECT"
PROJECT = "PROJECT"
REGION = "REGION"
GCS_STAGING_LOCATION = "GCS_STAGING_LOCATION"
SUBNET = "SUBNET"
IS_PARAMETERIZED = "IS_PARAMETERIZED"


# MySQL to Spanner

## Arguments
OUTPUT_NOTEBOOK_ARG = "output.notebook"
MAX_PARALLELISM_ARG = "max.parallelism"
MYSQL_HOST_ARG = "mysql.host"
MYSQL_PORT_ARG = "mysql.port"
MYSQL_USERNAME_ARG = "mysql.username"
MYSQL_PASSWORD_ARG = "mysql.password"
MYSQL_DATABASE_ARG = "mysql.database"
# leave list empty for migrating complete database else provide tables as 'table1,table2'
MYSQLTABLE_LIST_ARG = "mysql.table_list"
# one of overwrite|append (Use append when schema already exists in Spanner)
MYSQL_OUTPUT_SPANNER_MODE_ARG = "mysql.output.spanner.mode"
SPANNER_INSTANCE_ARG = "spanner.instance"
SPANNER_DATABASE_ARG = "spanner.database"
# provide table & pk column which do not have PK in MYSQL "{"table_name":"primary_key"}"
SPANNER_TABLE_PRIMARY_KEYS_ARG = "spanner.table.primary.keys"

## Constants
MAX_PARALLELISM = "MAX_PARALLELISM"
MYSQL_HOST = "MYSQL_HOST"
MYSQL_PORT = "MYSQL_PORT"
MYSQL_USERNAME = "MYSQL_USERNAME"
MYSQL_PASSWORD = "MYSQL_PASSWORD"
MYSQL_DATABASE = "MYSQL_DATABASE"
MYSQLTABLE_LIST = "MYSQLTABLE_LIST"
MYSQL_OUTPUT_SPANNER_MODE = "MYSQL_OUTPUT_SPANNER_MODE"
SPANNER_INSTANCE = "SPANNER_INSTANCE"
SPANNER_DATABASE = "SPANNER_DATABASE"
SPANNER_TABLE_PRIMARY_KEYS = "SPANNER_TABLE_PRIMARY_KEYS"
