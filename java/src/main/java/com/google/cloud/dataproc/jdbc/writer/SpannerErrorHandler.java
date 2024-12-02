/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataproc.jdbc.writer;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SpannerErrorHandler class handles errors encountered during Spanner operations. It logs the
 * errors and writes the error details to a specified GCS bucket.
 */
public class SpannerErrorHandler {

  // Logger for logging events and error messages
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerErrorHandler.class);

  // GCS bucket name where error logs will be written
  private final String gcsBucketName;

  // GCS directory path (optional)
  private final String gcsDirectoryPath;

  // GCS Storage client
  private final Storage storage;

  /**
   * Constructor to initialize SpannerErrorHandler with the specified GCS bucket name and directory
   * path.
   *
   * @param gcsBucketName The name of the GCS bucket for storing error logs.
   * @param gcsDirectoryPath The directory path within the GCS bucket for storing logs.
   */
  public SpannerErrorHandler(String gcsBucketName, String gcsDirectoryPath) {
    this.gcsBucketName = gcsBucketName;
    this.gcsDirectoryPath = gcsDirectoryPath != null ? gcsDirectoryPath : "";
    this.storage = StorageOptions.getDefaultInstance().getService();
  }

  /**
   * Handles errors based on their type and writes the error details to GCS.
   *
   * @param e The exception that was thrown.
   * @param rowData The data of the row that caused the error.
   */
  public void handleError(Exception e, String rowData) {
    String errorCode = getErrorCode(e);

    // Handle specific error cases
    switch (errorCode) {
      case "ALREADY_EXISTS":
        logAndWriteError("Row skipped due to ALREADY_EXISTS error: " + rowData);
        break;
      case "ABORTED":
        logAndWriteError("Row skipped due to ABORTED error: " + rowData);
        break;
      case "CANCELLED":
        logAndWriteError("Row skipped due to CANCELLED error: " + rowData);
        break;
      default:
        LOGGER.error("Unhandled error: {} | Message: {}", errorCode, e.getMessage());
        logAndWriteError("Unhandled error: " + e.getMessage() + " | Row data: " + rowData);
    }
  }

  /**
   * Logs the error message and writes it to a GCS bucket.
   *
   * @param errorMessage The error message to be logged and stored.
   */
  private void logAndWriteError(String errorMessage) {
    LOGGER.warn(errorMessage);
    writeErrorToGCS(errorMessage);
  }

  /**
   * Writes the error message to a GCS bucket.
   *
   * @param errorMessage The error message to be written.
   */
  private void writeErrorToGCS(String errorMessage) {
    try {
      // Generate a unique file name for each error log
      String fileName = gcsDirectoryPath + "error_log_" + System.currentTimeMillis() + ".txt";

      // Create a BlobId for the GCS object
      BlobId blobId = BlobId.of(gcsBucketName, fileName);

      // Create a BlobInfo object
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();

      // Upload the error message as a blob to GCS
      storage.create(blobInfo, errorMessage.getBytes());

      LOGGER.info("Error message successfully written to GCS: gs://{}/{}", gcsBucketName, fileName);
    } catch (Exception e) {
      LOGGER.error("Failed to write to GCS: {}", e.getMessage());
    }
  }

  /**
   * Extracts an error code from the exception. This is a placeholder for more specific error
   * handling.
   *
   * @param e The exception to analyze.
   * @return A string representing the error code or class name.
   */
  private String getErrorCode(Exception e) {
    // Return the simple class name of the exception as a placeholder for error codes
    return e.getClass().getSimpleName();
  }
}
