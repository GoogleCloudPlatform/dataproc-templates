from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

def add_insertion_time_column(df: DataFrame) -> DataFrame:
    """
    Adds a new column 'insertion_time' to the PySpark DataFrame with the current timestamp.

    Args:
        df: The input PySpark DataFrame.

    Returns:
        A new PySpark DataFrame with the 'insertion_time' column added.
    """
    return df.withColumn("insertion_time", current_timestamp())

if __name__ == "__main__":
    # This is a simple example to demonstrate the function.
    # In a real scenario, you would have an active SparkSession.
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        spark = SparkSession.builder.appName("TimestampColumnExample").getOrCreate()

        # Create a sample DataFrame
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        sample_df = spark.createDataFrame(data, schema)

        print("Original DataFrame:")
        sample_df.show()

        # Add the insertion time column
        df_with_time = add_insertion_time_column(sample_df)

        print("\nDataFrame with insertion_time column:")
        df_with_time.show()
        df_with_time.printSchema()

        spark.stop()
    except ImportError:
        print("PySpark is not installed. Please install PySpark to run the example:")
        print("pip install pyspark")
    except Exception as e:
        print(f"An error occurred: {e}")
