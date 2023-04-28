# Common

# max number of tables which will migrated parallelly
MAX_PARALLELISM = "MAX_PARALLELISM"

# MySQL to Spanner 
MYSQL_HOST = "MYSQL_HOST"
MYSQL_PORT = "MYSQL_PORT"
MYSQL_USERNAME = "MYSQL_USERNAME"
MYSQL_PASSWORD = "MYSQL_PASSWORD"
MYSQL_DATABASE = "MYSQL_DATABASE"
# leave list empty for migrating complete database else provide tables as 'table1,table2'
MYSQLTABLE_LIST = "MYSQLTABLE_LIST"
# one of overwrite|append (Use append when schema already exists in Spanner)
MYSQL_OUTPUT_SPANNER_MODE = "MYSQL_OUTPUT_SPANNER_MODE"
SPANNER_INSTANCE = "SPANNER_INSTANCE"
SPANNER_DATABASE = "SPANNER_DATABASE"
# provide table & pk column which do not have PK in MYSQL "{"table_name":"primary_key"}"
SPANNER_TABLE_PRIMARY_KEYS = "SPANNER_TABLE_PRIMARY_KEYS"
