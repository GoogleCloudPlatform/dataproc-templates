
import argparse
from pyspark.sql import SparkSession, DataFrame
from data_transformer import add_insertion_time_column

def transform_data(spark: SparkSession, input_path: str, output_path: str):
    """
    Reads data from a Parquet file, adds an insertion time column, and writes to an Avro file.

    Args:
        spark: The SparkSession object.
        input_path: The GCS path to the input Parquet data.
        output_path: The GCS path to write the output Avro data.
    """
    # Read the Parquet data from GCS
    input_df = spark.read.parquet(input_path)

    # Add the insertion time column
    transformed_df = add_insertion_time_column(input_df)

    # Write the transformed data to GCS in Avro format
    # You need to have the spark-avro package.
    # Example for spark-submit:
    # --packages org.apache.spark:spark-avro_2.12:3.3.0
    transformed_df.write.format("avro").save(output_path)

    print(f"Data successfully transformed and saved to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark data transformation script")
    parser.add_argument("--input", required=True, help="Input GCS path for Parquet data")
    parser.add_argument("--output", required=True, help="Output GCS path for Avro data")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Parquet to Avro Transformation").getOrCreate()

    transform_data(spark, args.input, args.output)

    spark.stop()
