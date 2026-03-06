
import argparse
from pyspark.sql import SparkSession
from data_transformer import add_insertion_time_column

def transform_hive_to_bigquery(spark: SparkSession, hive_database: str, hive_table: str, bq_table: str, bq_temp_gcs_bucket: str):
    """
    Reads a Hive table, adds an insertion_time column, and writes to a BigQuery table.

    Args:
        spark: The SparkSession object.
        hive_database: The name of the source Hive database.
        hive_table: The name of the source Hive table.
        bq_table: The destination BigQuery table (e.g., 'dataset.table').
        bq_temp_gcs_bucket: The GCS bucket for temporary BigQuery connector data.
    """
    # Read data from Hive table
    input_df = spark.table(f'{hive_database}.{hive_table}')

    # Add the insertion time column
    transformed_df = add_insertion_time_column(input_df)

    # Write the transformed data to BigQuery
    transformed_df.write \
        .format('bigquery') \
        .option('table', bq_table) \
        .option('temporaryGcsBucket', bq_temp_gcs_bucket) \
        .mode('append') \
        .save()

    print(f"Data successfully written to BigQuery table: {bq_table}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PySpark Hive to BigQuery transformation script')
    parser.add_argument('--hive_database', required=True, help='Source Hive database')
    parser.add_argument('--hive_table', required=True, help='Source Hive table')
    parser.add_argument('--bq_table', required=True, help='Destination BigQuery table (dataset.table)')
    parser.add_argument('--bq_temp_gcs_bucket', required=True, help='GCS bucket for temporary BigQuery connector data')
    args = parser.parse_args()

    # Create a SparkSession with Hive support
    spark = SparkSession.builder \
        .appName('Hive to BigQuery Transformation') \
        .enableHiveSupport() \
        .getOrCreate()

    transform_hive_to_bigquery(spark, args.hive_database, args.hive_table, args.bq_table, args.bq_temp_gcs_bucket)

    spark.stop()
