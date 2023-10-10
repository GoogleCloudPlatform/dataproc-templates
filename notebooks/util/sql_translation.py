from google.cloud import bigquery_migration_v2
import logging

def create_migration_workflow(
    gcs_input_path: str, gcs_output_path: str, project_id: str, 
    bq_dataset: str, default_database: str, source_dilect: str, bq_region: str,
    obj_name_mapping: str = None) -> None:
    """
    This function uses BQ translation API to convert DDLs/SQLs from different sources to BQ

    Args:
        gcs_input_path (string): Cloud Storage location where source DDLs are available
        gcs_output_path (string): Output Cloud Storage location
        project_id (string): Project ID
        bq_dataset (string): BQ Dataset ID to be added in the final DDL
        default_database (string): Project ID to be added in the final DDL
        source_dilect (string): Can be hive|redshift|netezza|teradata|synapse|mysql|oracle|postgresql|presto|sparksql|SQLserver|vertica
        bq_region: Region of BQ Dataset
        obj_name_mapping: The mapping of objects to their desired output names in list form.

    Returns:
        name (string): Full name of the Migration Job
        state (string): Job Run State

    """
    """Creates a migration workflow of a Batch SQL Translation and prints the response."""
    LOGGER: logging.Logger = logging.getLogger('dataproc_templates')
    parent = f"projects/{project_id}/locations/{bq_region}"
    # Construct a BigQuery Migration client object.
    client = bigquery_migration_v2.MigrationServiceClient()
    source_dialect = bigquery_migration_v2.Dialect()
    # Set the source dialect to Hive SQL.
    if source_dilect == "hive":
        source_dialect.hiveql_dialect = bigquery_migration_v2.HiveQLDialect()
        migration_type="Translation_HiveQL2BQ"
    # Set the target dialect to BigQuery dialect.
    target_dialect = bigquery_migration_v2.Dialect()
    target_dialect.bigquery_dialect = bigquery_migration_v2.BigQueryDialect()
    # Prepare the config proto.
    translation_config = bigquery_migration_v2.TranslationConfigDetails(
        gcs_source_path=gcs_input_path,
        gcs_target_path=gcs_output_path,
        source_dialect=source_dialect,
        target_dialect=target_dialect,
        source_env={"default_database": default_database,
                     "schema_search_path":{
                      bq_dataset
                    }
                    },
        name_mapping_list=obj_name_mapping
       )
    # Prepare the task.
    migration_task = bigquery_migration_v2.MigrationTask(
        type_=migration_type, translation_config_details=translation_config)
    # Prepare the workflow.
    workflow = bigquery_migration_v2.MigrationWorkflow(
        display_name=f"workflow-python-{source_dilect}2bq")
    workflow.tasks["translation-task"] = migration_task  # type: ignore
    # Prepare the API request to create a migration workflow.
    request = bigquery_migration_v2.CreateMigrationWorkflowRequest(
        parent=parent,
        migration_workflow=workflow,)
    response = client.create_migration_workflow(request=request)
    LOGGER.info("Created workflow: "+str(response.display_name))
    LOGGER.info("Current state: "+str(response.State(response.state)))
    return response.name, response.state
       

def get_migration_workflow_status(name):
    """
    This function returns current running state of BQ translation API job

    Args:
        name (string): Full name of the Migration Job
    Returns:
        state (string): Job Run State

    """
    LOGGER: logging.Logger = logging.getLogger('dataproc_templates')
    
    # Create a client
    client = bigquery_migration_v2.MigrationServiceClient()
    # Initialize request argument(s)
    request = bigquery_migration_v2.GetMigrationWorkflowRequest(
        name=name,
    )
    # Make the request
    response = client.get_migration_workflow(request=request)
    LOGGER.info(str(response.state))
    # Return the response
    return response
