# Dataproc Templates - PySpark Hive to BigQuery

This directory contains a PySpark script for transforming data from Hive and loading it into BigQuery.

---
The text between the line above and line below was written by a human. The rest of the document was created by Gemini. The initial prompt to Gemini was:
```
    Create a PySpark script to extract and transform a hive table, adding an insertion_time column using the add_insertion_time_column function in @data_tranformer.py. Save this table to BigQuery, providing detailed instructions to run this script against a dataproc cluster. Save a summary of this session to hive_to_BQReadme.md
```
Gemini generated the Pyspark script, specifically the file `transform_hive_to_bigquery.py` and the README file. Minor changes were required to run the script because a) the preinstalled Spark BigQuery connector in cluster versions 2.1 and higher was used and b) the Dataproc Hive Metastore service with the thrift protocol was used. The working script is
```
gcloud dataproc jobs submit pyspark gs://path_to_src/transform_hive_to_bigquery.py \
    --cluster=<cluster-name> --py-files=gs://path_to_src/data_transformer.py \
    --properties=spark.hadoop.hive.metastore.uris=<URI> \
    -- --hive_database=<database> --hive_table=<table> --bq_table=<dataset>.<table> \
    --bq_temp_gcs_bucket=<temp-bucket-name>
```
---
## Scripts

*   `data_transformer.py`: A module that contains functions for data transformation.
*   `transform_hive_to_bigquery.py`: A PySpark script that reads a table from Hive, adds an `insertion_time` column, and writes the transformed data to a BigQuery table.

## How to run the script

To run the `transform_hive_to_bigquery.py` script, you can use the `gcloud dataproc jobs submit pyspark` command. You will need to provide the Hive and BigQuery table details.

**Prerequisites:**

1.  A running Dataproc cluster.
2.  The BigQuery Connector for Spark must be available on the cluster. You can add it using the `--jars` flag when submitting the job.
3.  The script `transform_hive_to_bigquery.py` and the module `data_transformer.py` must be accessible to the job. The easiest way to do this is to upload them to a GCS bucket and then specify them using the `--py-files` flag.

**Example:**

```bash
gcloud dataproc jobs submit pyspark gs://<your-bucket>/path/to/transform_hive_to_bigquery.py \
    --cluster=<your-cluster-name> \
    --region=<your-region> \
    --py-files=gs://<your-bucket>/path/to/data_transformer.py \
    --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar \
    -- \
    --hive_database=<your-hive-database> \
    --hive_table=<your-hive-table> \
    --bq_table=<your-bq-dataset>.<your-bq-table> \
    --bq_temp_gcs_bucket=<your-gcs-bucket-for-temp-data>
```

### **Argument Reference:**

*   `--hive_database`: The name of the source Hive database.
*   `--hive_table`: The name of the source Hive table.
*   `--bq_table`: The destination BigQuery table in `dataset.table` format.
*   `--bq_temp_gcs_bucket`: A GCS bucket used by the BigQuery connector to temporarily store data before writing it to BigQuery.

## Session Summary

This session involved creating a PySpark script to extract a Hive table, transform it by adding an `insertion_time` column, and load it into BigQuery. The `add_insertion_time_column` function was reused from the existing `data_transformer.py` module. A README file was also created to provide detailed instructions on how to run this script on a Dataproc cluster, including the necessary `gcloud` command and argument explanations.
