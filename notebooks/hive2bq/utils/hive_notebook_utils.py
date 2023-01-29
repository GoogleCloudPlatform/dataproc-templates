from pyspark.sql import SparkSession
from google.cloud import storage
from datetime import datetime, timedelta
import time
from google.cloud import storage
from google.cloud import bigquery

def get_spark_session(HIVE_METASTORE):
    spark = SparkSession \
    .builder \
    .appName("extract_hive_ddl") \
    .config("hive.metastore.uris", HIVE_METASTORE) \
    .config('spark.sql.debug.maxToStringFields', 2000) \
    .enableHiveSupport() \
    .getOrCreate()
    return spark


def WriteToCloud ( ddls_combined,bucket,db_name ):
  client = storage.Client()
  bucket = client.get_bucket( bucket )
  blob = bucket.blob(f"hive_ddls/input/{db_name}/{db_name}.sql" )
  blob.upload_from_string( ddls_combined )
    
    
#get_hive_ddls(INPUT_HIVE_DATABASE,TABLE_LIST,TEMP_BUCKET,spark)
def get_hive_ddls(INPUT_HIVE_DATABASE,TABLE_LIST,TEMP_BUCKET,spark):
    ddls_combined=""
    for tbl in TABLE_LIST:
        ddl_hive=""
        print(INPUT_HIVE_DATABASE)
        print(tbl)
        try:
            print(spark.sql(f"show create table  {INPUT_HIVE_DATABASE}.{tbl} as serde").first()[0])
            ddl_hive=spark.sql(f"show create table  {INPUT_HIVE_DATABASE}.{tbl} as serde").first()[0].split("\nLOCATION '")[0]
        except Exception as e:
            print(e)
        if len(ddl_hive)<1:
            try:
                ddl_hive=spark.sql(f"show create table  {INPUT_HIVE_DATABASE}.{tbl}").first()[0].split("\nUSING ")[0]
            except Exception as e:
                print(e)
        if len(ddl_hive)<1:
            print(f"Could not get DDL for Table: {tbl}")
        ddl_hive=ddl_hive.replace(f"{INPUT_HIVE_DATABASE}.","").replace(f" TABLE {tbl}",f" TABLE IF NOT EXISTS {tbl}")+";\n\n"
        ddls_combined=ddls_combined+ddl_hive
    WriteToCloud( ddls_combined,TEMP_BUCKET,INPUT_HIVE_DATABASE )



def create_migration_workflow(
    gcs_input_path: str, gcs_output_path: str, project_id: str, bq_dataset: str) -> None:
    from google.cloud import bigquery_migration_v2
    """Creates a migration workflow of a Batch SQL Translation and prints the response."""
    parent = f"projects/{project_id}/locations/us"
    # Construct a BigQuery Migration client object.
    client = bigquery_migration_v2.MigrationServiceClient()
    # Set the source dialect to Hive SQL.
    source_dialect = bigquery_migration_v2.Dialect()
    source_dialect.hiveql_dialect = bigquery_migration_v2.HiveQLDialect()
    # Set the target dialect to BigQuery dialect.
    target_dialect = bigquery_migration_v2.Dialect()
    target_dialect.bigquery_dialect = bigquery_migration_v2.BigQueryDialect()
    # Prepare the config proto.
    translation_config = bigquery_migration_v2.TranslationConfigDetails(
        gcs_source_path=gcs_input_path,
        gcs_target_path=gcs_output_path,
        source_dialect=source_dialect,
        target_dialect=target_dialect,
        source_env={"default_database": project_id,
                     "schema_search_path":{ bq_dataset }
                    }
       )
    # Prepare the task.
    migration_task = bigquery_migration_v2.MigrationTask(
        type_="Translation_HiveQL2BQ", translation_config_details=translation_config)
    # Prepare the workflow.
    workflow = bigquery_migration_v2.MigrationWorkflow(
        display_name="demo-workflow-python-example-Hive2BQ")
    workflow.tasks["translation-task"] = migration_task  # type: ignore
    # Prepare the API request to create a migration workflow.
    request = bigquery_migration_v2.CreateMigrationWorkflowRequest(
        parent=parent,
        migration_workflow=workflow,)
    response = client.create_migration_workflow(request=request)
    print("Created workflow: "+str(response.display_name))
    request = bigquery_migration_v2.GetMigrationWorkflowRequest(
                    name=response.name,
                )
    while ('RUNNING' in response.state.name):
        response = client.get_migration_workflow(request=request )
        print("Current state: RUNNING")
        time.sleep(5)
    print("Current state: "+response.state.name)

# create_bq_tables(TEMP_BUCKET,INPUT_HIVE_DATABASE)
def create_bq_tables(TEMP_BUCKET,INPUT_HIVE_DATABASE):

    client = storage.Client()
    bucket = client.get_bucket(TEMP_BUCKET)
    query_file_uri = f"hive_ddls/output/{INPUT_HIVE_DATABASE}/{INPUT_HIVE_DATABASE}.sql"
    blob = bucket.get_blob(query_file_uri)
    query=blob.download_as_text()
    client = bigquery.Client()
    results=query_job = client.query(
        query=query)
    print(results)
    
