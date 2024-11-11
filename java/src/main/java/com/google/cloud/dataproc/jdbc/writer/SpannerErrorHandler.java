package com.google.cloud.dataproc.jdbc.writer;

import com.google.cloud.spanner.SpannerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * SpannerErrorHandler class handles errors encountered during Spanner operations.
 * It logs the errors and writes the error details to a specified location.
 */
public class SpannerErrorHandler {

  // Logger for logging events and error messages
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerErrorHandler.class);

  // Path to the GCS bucket where error logs will be written
  private final String gcsBucketPath;

  /**
   * Constructor to initialize SpannerErrorHandler with the specified GCS bucket path.
   *
   * @param gcsBucketPath The path to the GCS bucket for storing error logs.
   */
  public SpannerErrorHandler(String gcsBucketPath) {
    this.gcsBucketPath = gcsBucketPath;
  }

  /**
   * Handles Spanner errors based on the error code.
   * It logs a warning message for the error and writes the error details to GCS.
   *
   * @param e       The SpannerException that was thrown.
   * @param rowData The data of the row that caused the error.
   */
  public void SpannerErrorHandler(SpannerException e, String rowData) {
    // Retrieve the error code from the SpannerException
    String errorCode = e.getErrorCode().name();

    // Determine the type of error and handle it accordingly
    switch (errorCode) {
      case "ALREADY_EXISTS":
        logAndWriteError("Row skipped due to ALREADY_EXISTS error: {}", rowData);
        break;
      case "ABORTED":
        logAndWriteError("Row skipped due to ABORTED error: {}", rowData);
        break;
      case "CANCELLED":
        logAndWriteError("Row skipped due to CANCELLED error: {}", rowData);
        break;
      default:
        // Log an error message for any unhandled Spanner error
        LOGGER.error("Unhandled Spanner error (code: {}): {}", errorCode, e.getMessage());
    }
  }

  /**
   * Logs the error message and writes it to GCS.
   *
   * @param message The message format string.
   * @param rowData The data of the row that caused the error.
   */
  private void logAndWriteError(String message, String rowData) {
    String errorMessage = String.format(message, rowData);
    LOGGER.warn(errorMessage);
    writeErrorToGCS(errorMessage);
  }

  /**
   * Writes the error message to a GCS bucket (simulated as a local file).
   *
   * @param errorMessage The error message to be logged.
   */
  private void writeErrorToGCS(String errorMessage) {
    // Create the full path to the error log file
    String filePath = Paths.get(gcsBucketPath, "error_log.txt").toString();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
      writer.write(errorMessage); // Write the error message to the file
      writer.newLine(); // Add a new line after the message
    } catch (IOException ioException) {
      // Log an error if writing to GCS fails
      LOGGER.error("Failed to write to GCS: {}", ioException.getMessage());
    }
  }
}
