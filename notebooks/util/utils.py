def create_migration_workflow(
    gcs_input_path: str, gcs_output_path: str, project_id: str, 
    bq_dataset: str, default_database: str, source_dilect: str, bq_region: str,
    obj_name_mapping: str = None) -> None:
    """
    obj_name_mapping:The mapping of objects to their desired output names in list form.
    """
    from google.cloud import bigquery_migration_v2
    """Creates a migration workflow of a Batch SQL Translation and prints the response."""
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
    print("Created workflow:")
    print(response.display_name)
    print("Current state:")
    print(response.State(response.state))
    return response.name, response.state
       

def get_migration_workflow_status(name):
    # Create a client
    from google.cloud import bigquery_migration_v2
    client = bigquery_migration_v2.MigrationServiceClient()
    # Initialize request argument(s)
    request = bigquery_migration_v2.GetMigrationWorkflowRequest(
        name=name,
    )
    # Make the request
    response = client.get_migration_workflow(request=request)
    # Return the response
    return response

def get_gcs_file_as_string(bucket,path):
    import io
    import os
    from google.cloud import storage
    # Create a storage client object.
    client = storage.Client()
    # Get the bucket object for the bucket that contains the text file.
    bucket = client.get_bucket(bucket)
    # Get the blob object for the text file.
    blob = bucket.blob(path)
    # Read the text file into a string.
    text = blob.download_as_string().decode("utf-8")
    # Return the text.
    return text

def run_bq_query(query):
    # Import the necessary modules.
    import io
    import os
    from google.cloud import bigquery
    # Create a BigQuery client object.
    client = bigquery.Client()
    # Run the SQL query.
    job = client.query(query)
    # Wait for the job to finish.
    job.result()
    # Print the results of the query.
    results = job.to_dataframe()
    return results