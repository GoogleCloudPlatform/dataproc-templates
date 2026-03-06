package com.customer.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class DataframeUtils {

    /**
     * Adds an "insertion_time" column to the given Spark DataFrame with the current timestamp.
     *
     * @param dataframe The input Spark DataFrame.
     * @return A new Spark DataFrame with the "insertion_time" column added.
     */
    public static Dataset<Row> addInsertionTimeColumn(Dataset<Row> dataframe) {
        return dataframe.withColumn("insertion_time", functions.current_timestamp());
    }
}
