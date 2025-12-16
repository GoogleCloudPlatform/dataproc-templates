# Dataproc Templates - PySpark Data Transformation

This directory contains a PySpark script for transforming data on Google Cloud Dataproc.

## Scripts

*   `data_transformer.py`: A module that contains functions for data transformation.
*   `transform_parquet_to_avro.py`: A PySpark script that reads data in Parquet format from a GCS bucket, adds an `insertion_time` column, and writes the transformed data to another GCS bucket in Avro format.

## How to run the script

To run the `transform_parquet_to_avro.py` script, you can use the `gcloud dataproc jobs submit pyspark` command. You will need to provide the input and output GCS paths.

**Example:**

```bash
gcloud dataproc jobs submit pyspark transform_parquet_to_avro.py \
    --cluster=<your-cluster-name> \
    --region=<your-region> \
    --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar \
    -- \
    --input=gs://<your-input-bucket>/path/to/parquet \
    --output=gs://<your-output-bucket>/path/to/avro
```

**Note:** You need to have the `spark-avro` package available in your cluster. You can add it using the `--packages` option when submitting the job, for example: `--packages org.apache.spark:spark-avro_2.12:3.3.0`. Alternatively, you can install it on your cluster nodes.
You also need to make sure that the `data_transformer.py` file is available to your PySpark job. You can either use `gcloud dataproc jobs submit pyspark --py-files` to ship it with your job, or install it as a package on the cluster.
For more information, see the [Dataproc documentation](https://cloud.google.com/dataproc/docs).
