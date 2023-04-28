def create_migration_workflow(
    gcs_input_path: str, gcs_output_path: str, project_id: str, bq_dataset: str, default_database: str, source_dilect: str, bq_region: str) -> None:
    from google.cloud import bigquery_migration_v2
    """Creates a migration workflow of a Batch SQL Translation and prints the response."""
    parent = f"projects/{project_id}/locations/{bq_region}"
    # Construct a BigQuery Migration client object.
    client = bigquery_migration_v2.MigrationServiceClient()
    
    source_dialect = bigquery_migration_v2.Dialect()
    # Set the source dialect to Hive SQL.
    if source_dilect == "hive":
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
        source_env={"default_database": default_database,
                     "schema_search_path":{
                      bq_dataset
                    }
                    },
       )
    print(translation_config)
    # Prepare the task.
    migration_task = bigquery_migration_v2.MigrationTask(
        type_=f"Translation_{source_dilect}QL2BQ", translation_config_details=translation_config)
    # Prepare the workflow.
    workflow = bigquery_migration_v2.MigrationWorkflow(
        display_name=f"workflow-python-{source_dilect}2bq")
    workflow.tasks["translation-task"] = migration_task  # type: ignore
    # Prepare the API request to create a migration workflow.
    request = bigquery_migration_v2.CreateMigrationWorkflowRequest(
        parent=parent,
        migration_workflow=workflow,)
    response = client.create_migration_workflow(request=request)
    print("Created workflow:")
    print(response.display_name)
    print("Current state:")
    print(response.State(response.state))
       

