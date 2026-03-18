
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def transform_bigquery_to_elasticsearch(
    spark: SparkSession,
    bq_table: str,
    bq_temp_gcs_bucket: str,
    sql_statement: str,
    es_nodes: str,
    es_port: str,
    es_index: str,
    es_api_key: str,
    es_ssl: str,
    es_wan_only: str
):
    """
    Reads data from BigQuery, applies a Spark SQL transformation, and writes to Elasticsearch.

    Args:
        spark: The SparkSession object.
        bq_table: The source BigQuery table (e.g., 'project.dataset.table').
        bq_temp_gcs_bucket: GCS bucket for temporary BigQuery connector data.
        sql_statement: Spark SQL statement to transform data in flight.
        es_nodes: Elasticsearch nodes (comma-separated).
        es_port: Elasticsearch port.
        es_index: Destination Elasticsearch index.
        es_api_key: Base64-encoded API key for Elasticsearch authentication.
        es_ssl: Whether to use SSL for Elasticsearch.
        es_wan_only: Whether to use WAN-only mode for Elasticsearch.
    """
    # Read data from BigQuery
    input_df = spark.read \
       .format("bigquery") \
       .option("table", bq_table) \
       .option("temporaryGcsBucket", bq_temp_gcs_bucket) \
       .load()

    # Create a temporary view for SQL transformation
    # The SQL statement should use 'source_table' as the table name.
    input_df.createOrReplaceTempView("source_table")

    # Apply the SQL transformation
    transformed_df = spark.sql(sql_statement)

    # Write the transformed data to Elasticsearch
    # Authenticate using the 'Authorization' header with 'ApiKey <base64_key>'
    transformed_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", es_nodes) \
        .option("es.port", es_port) \
        .option("es.resource", es_index) \
        .option("es.net.ssl", es_ssl) \
        .option("es.nodes.wan.only", es_wan_only) \
        .option("es.net.http.header.Authorization", f"ApiKey {es_api_key}") \
        .mode("append") \
        .save()

    print(f"Data successfully migrated from BigQuery table '{bq_table}' to Elasticsearch index '{es_index}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='PySpark BigQuery to Elasticsearch migration script')
    parser.add_argument('--bq_table', required=True, help='Source BigQuery table (project.dataset.table)')
    parser.add_argument('--bq_temp_gcs_bucket', required=True, help='GCS bucket for temporary BigQuery connector data')
    parser.add_argument('--sql_statement', required=True, help='Spark SQL statement for in-flight transformation')
    parser.add_argument('--es_nodes', required=True, help='Elasticsearch nodes (comma-separated)')
    parser.add_argument('--es_port', default='443', help='Elasticsearch port (default: 443)')
    parser.add_argument('--es_index', required=True, help='Destination Elasticsearch index')
    parser.add_argument('--es_api_key', required=True, help='Base64-encoded Elasticsearch API key')
    parser.add_argument('--es_ssl', default='true', help='Use SSL for Elasticsearch (default: true)')
    parser.add_argument('--es_wan_only', default='true', help='Use WAN-only mode for Elasticsearch (default: true)')

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName('BigQuery to Elasticsearch Migration') \
        .getOrCreate()

    transform_bigquery_to_elasticsearch(
        spark,
        args.bq_table,
        args.bq_temp_gcs_bucket,
        args.sql_statement,
        args.es_nodes,
        args.es_port,
        args.es_index,
        args.es_api_key,
        args.es_ssl,
        args.es_wan_only
    )

    spark.stop()
