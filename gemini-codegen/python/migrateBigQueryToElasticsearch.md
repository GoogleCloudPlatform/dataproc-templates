# Migrate BigQuery to Elasticsearch using PySpark on Dataproc Serverless

This guide explains how to run the `transform_bigquery_to_elasticsearch.py` script on Google Cloud Dataproc Serverless.

---
The text between the line above and line below was written by a human. The rest of the document was created by Gemini. The initial prompt to Gemini was:
```
    Create a pySpark script to migrate data from BigQuery to Elastisearch authenticating with an API key. Modify the data in flight with a SparkSQL statement provided. Provide instructions to run this job on serverless spark in migrateBigQueryToElasticsearch.md and provide a summary of the session in migrateBigQueryToElasticsearchREADME.md
```
Gemini generated the Pyspark script, specifically the file `transform_bigquery_to_elasticsearch.py` and the README file. Changes were required to run the script. The working gcloud command is:
```
    gcloud dataproc batches submit pyspark transform_bigquery_to_elasticsearch.py --version=<version> \
    --files=<trusted-ca>.jks --project=<PROJECT_ID> --region=<REGION> --deps-bucket=<dependency-bucket> \
    --jars=<elasticsearch-spark-version-scala-version-elastic-version.jar in GCS> \
    --properties="spark.driver.extraJavaOptions=-Djavax.net.ssl.trustStore=<trusted-ca>.jks -Djavax.net.ssl.trustStorePassword=changeit,spark.executor.extraJavaOptions=-Djavax.net.ssl.trustStore=<trusted-ca>.jks -Djavax.net.ssl.trustStorePassword=changeit" \
    -- \
    --bq_table=dataproc-templates:gemini_codegen.bq_to_es --sql_statement="<SQL_QUERY>" \
    --es_nodes=<node-ips> --es_port=<usually 9200> --es_index=<index-name> \
    --es_api_key=<ES_API_KEY> --es_ssl=true --es_wan_only=true \
    --bq_temp_gcs_bucket=<temporary-bucket>
```
The `batches` command required updates, such as replacing `--packages` with `--jars`. To facilitate a secure SSL handshake between Spark containers and Elasticsearch, the truststore must include the Elasticsearch self-signed certificate. Additionally, it must incorporate the Spark container's default truststore to maintain SSL compatibility with BigQuery and the spark environment.

A recommended approach is to use a PySpark script to export the default container truststore to Google Cloud Storage (GCS). The Elasticsearch certificate can then be added as a trusted CA using the Java `keytool` utility. Finally, the unified truststore is passed to the Spark runtime via the `-Djavax.net.ssl.trustStore` argument.

---

## Prerequisites

1.  **GCP Project**: Ensure you have a GCP project with Dataproc and BigQuery APIs enabled.
2.  **GCS Bucket**: You need a GCS bucket for temporary BigQuery data and for staging dependencies.
3.  **Elasticsearch Instance**: An accessible Elasticsearch cluster (e.g., Elastic Cloud or self-managed).
4.  **Elasticsearch API Key**: A Base64-encoded API key for authentication.
5.  **Connectors**:
    *   **BigQuery Connector**: `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12`
    *   **Elasticsearch Connector**: `org.elasticsearch:elasticsearch-spark-30_2.12`

## Running the Job

Use the `gcloud dataproc batches submit pyspark` command to submit the job.

### Example Command

```bash
# Variables
PROJECT_ID="your-project-id"
REGION="your-region"
BUCKET="your-gcs-bucket"
BQ_TABLE="your-project.your_dataset.your_table"
ES_NODES="your-elasticsearch-endpoint"
ES_INDEX="your-index-name"
ES_API_KEY="your-base64-api-key"
SQL_QUERY="SELECT * FROM source_table WHERE status = 'active'"

gcloud dataproc batches submit pyspark transform_bigquery_to_elasticsearch.py \
    --project=$PROJECT_ID \
    --region=$REGION \
    --deps-bucket=gs://$BUCKET \
    --packages="org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1" \
    -- \
    --bq_table=$BQ_TABLE \
    --bq_temp_gcs_bucket=$BUCKET/temp \
    --sql_statement="$SQL_QUERY" \
    --es_nodes=$ES_NODES \
    --es_port=443 \
    --es_index=$ES_INDEX \
    --es_api_key=$ES_API_KEY \
    --es_ssl=true \
    --es_wan_only=true
```

## Parameter Details

| Parameter | Description |
| :--- | :--- |
| `--bq_table` | The source BigQuery table in `project.dataset.table` format. |
| `--bq_temp_gcs_bucket` | GCS bucket used by the BigQuery connector for temporary data export. |
| `--sql_statement` | Spark SQL query to transform data in flight. Use `source_table` as the table name in your query. |
| `--es_nodes` | Comma-separated list of Elasticsearch nodes or the Cloud ID endpoint. |
| `--es_port` | Elasticsearch port (default: 443 for HTTPS). |
| `--es_index` | The destination Elasticsearch index. |
| `--es_api_key` | Base64-encoded API key for Elasticsearch. |
| `--es_ssl` | Set to `true` for HTTPS connections. |
| `--es_wan_only` | Set to `true` if connecting to Elastic Cloud or via a load balancer. |

## Notes on Transformation

The script registers the BigQuery data as a temporary view named `source_table`. Your SQL statement must reference this name.

Example:
```sql
SELECT 
    id, 
    upper(name) as name, 
    current_timestamp() as processed_at 
FROM source_table
```
