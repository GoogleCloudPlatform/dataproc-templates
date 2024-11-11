package com.google.cloud.dataproc.jdbc.writer;

// Import necessary dependencies and classes
import static org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.createTable;
import static org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.dropTable;

import java.sql.Connection;
import java.sql.SQLException;

import com.google.cloud.spanner.SpannerException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.collection.immutable.Map;
import com.google.cloud.dataproc.jdbc.writer.SpannerErrorHandler;

/**
 * LenientJdbcWriter class implements CreatableRelationProvider, which provides a way to write
 * data from a Spark DataFrame to a JDBC data source, with lenient handling of SaveMode.
 */
public class LenientJdbcWriter implements CreatableRelationProvider {

  // Define a logger to log events and messages
  private static final Logger LOGGER = LoggerFactory.getLogger(LenientJdbcWriter.class);

  // Instantiate SpannerErrorHandler with a GCS bucket path
  private final SpannerErrorHandler errorHandler = new SpannerErrorHandler("gs://your-bucket-path");

  /**
   * Method to create a relation (i.e., write a DataFrame to a JDBC table) based on the provided SaveMode.
   * @param sqlContext The SQLContext associated with the DataFrame.
   * @param mode The SaveMode, which defines how to handle existing data.
   * @param parameters The JDBC connection parameters such as URL and table name.
   * @param df The DataFrame to write to the table.
   * @return A BaseRelation object representing the written data.
   */
  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> df) {
    // Extract JDBC options from parameters
    JdbcOptionsInWrite options = new JdbcOptionsInWrite(parameters);
    boolean isCaseSensitive = SparkSession.active().sessionState().conf().caseSensitiveAnalysis();
    JdbcDialect dialect = JdbcDialects.get(options.url());
    Connection conn = dialect.createConnectionFactory(options).apply(-1);

    try {
      // Check if the table exists
      boolean tableExists = JdbcUtils.tableExists(conn, options);

      if (tableExists) {
        // Handle different save modes when the table already exists
        switch (mode) {
          case Overwrite:
            // Drop the existing table and create a new one
            try {
              dropTable(conn, options.table(), options);
            } catch (SpannerException e) {
              errorHandler.SpannerErrorHandler(e, "Dropping table: " + options.table());
            }
            try {
              createTable(conn, options.table(), df.schema(), isCaseSensitive, options);
            } catch (SpannerException e) {
              errorHandler.SpannerErrorHandler(e, "Creating table: " + options.table());
            }
            saveTableLenient(df, options, isCaseSensitive, Option.apply(df.schema()));
            break;

          case Append:
            // Append data to the existing table
            try {
              Option<StructType> tableSchema = JdbcUtils.getSchemaOption(conn, options);
              saveTableLenient(df, options, isCaseSensitive, tableSchema);
            } catch (SpannerException e) {
              errorHandler.SpannerErrorHandler(e, "Appending to table: " + options.table());
            }
            break;

          case ErrorIfExists:
            throw new RuntimeException(
                String.format("Table or view '%s' already exists. SaveMode: ErrorIfExists.", options.table()));

          case Ignore:
            // Do nothing if table exists and mode is Ignore
            break;
        }
      } else {
        // Create the table if it doesn't exist and save the data
        try {
          createTable(conn, options.table(), df.schema(), isCaseSensitive, options);
          saveTableLenient(df, options, isCaseSensitive, Option.apply(df.schema()));
        } catch (SpannerException e) {
          errorHandler.SpannerErrorHandler(e, "Creating and saving to table: " + options.table());
        }
      }
    } finally {
      // Ensure the JDBC connection is closed
      try {
        conn.close();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    // Return a BaseRelation representing the data that was written
    return new BaseRelation() {
      @Override
      public SQLContext sqlContext() {
        return sqlContext;
      }

      @Override
      public StructType schema() {
        return df.schema();
      }
    };
  }

  /**
   * Saves the DataFrame data into the specified JDBC table with lenient handling.
   * @param df The DataFrame containing the data to write.
   * @param options The JDBC options for the write operation.
   * @param isCaseSensitive Boolean flag for case sensitivity in column names.
   * @param tableSchema The schema of the table, if available.
   */
  private void saveTableLenient(
      Dataset<Row> df,
      JdbcOptionsInWrite options,
      boolean isCaseSensitive,
      Option<StructType> tableSchema) {

    // Extract table and connection details
    String url = options.url();
    String table = options.table();
    JdbcDialect dialect = JdbcDialects.get(url);
    StructType rddSchema = df.schema();
    int batchSize = options.batchSize();
    int isolationLevel = options.isolationLevel();

    // Generate the SQL insert statement for writing data
    String insertStmt =
        JdbcUtils.getInsertStatement(table, rddSchema, tableSchema, isCaseSensitive, dialect);

    // Get the number of partitions for parallel writing
    Integer numPartitions = options.numPartitions().getOrElse(() -> 1);

    Dataset<Row> repartitionedDF = df;

    // Check if the number of partitions is valid
    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid value `%d` for parameter `%s` in table writing via JDBC. The minimum value is 1.",
              numPartitions, JDBCOptions.JDBC_NUM_PARTITIONS()));
    } else if (numPartitions < df.rdd().getNumPartitions()) {
      repartitionedDF = df.coalesce(numPartitions);
    }

    // Write the data to the JDBC table partition by partition
    try {
      repartitionedDF
          .javaRDD()
          .foreachPartition(
              new SavePartitionLenient(
                  table, rddSchema, insertStmt, batchSize, dialect, isolationLevel, options));
    } catch (SpannerException e) {
      errorHandler.SpannerErrorHandler(e, "Error during partition save for table: " + table);
    }
  }
}
