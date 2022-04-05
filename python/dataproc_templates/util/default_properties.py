from typing import Dict

from . import template_constants as constants

DEFAULT_PROPERTIES: Dict[str, str] = {
    constants.PROJECT_ID_PROP: '<project_id>',
    # GcsToBigquery properties
    constants.GCS_BQ_INPUT_FORMAT: '<avro,parquet,csv>',
    constants.GCS_BQ_INPUT_LOCATION: '<gs://input_path>',
    constants.GCS_BQ_OUTPUT_DATASET: '<dataset_name>',
    constants.GCS_BQ_OUTPUT_TABLE: '<table_name>',
    constants.GCS_BQ_LD_TEMP_BUCKET_NAME: '<temp_bucket_name>',
    # BigQueryToGcs properties
    constants.BQ_GCS_INPUT_TABLE: '<project:dataset.table>',
    constants.BQ_GCS_OUTPUT_FORMAT: '<avro,parquet,csv,json>',
    constants.BQ_GCS_OUTPUT_MODE: '<append,overwrite>',
    constants.BQ_GCS_OUTPUT_LOCATION: '<gcs-path>',
}
