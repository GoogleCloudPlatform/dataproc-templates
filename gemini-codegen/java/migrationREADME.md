# Postgres to MySQL Migration Spark Job

This project contains a Spark job written in Java to migrate data from a table in a Postgres database to a table in a MySQL database.

## Features

*   **Data Migration:** Migrates data from a specified Postgres table to a specified MySQL table.
*   **Credential Management:** Securely retrieves JDBC connection strings (including username and password) from Google Secret Manager.
*   **Parallel Processing:** Reads data from Postgres in parallel based on a specified partition column to improve performance.
*   **Batch Writing:** Writes data to MySQL in batches for improved efficiency and to avoid overwhelming the database.
*   **Data Transformation:** Adds an `insertion_time` column to the data with the current timestamp before writing it to the destination table.

## Project Structure

*   `src/main/java/com/customer/app/PostgresToMySql.java`: The main application file containing the Spark job logic.
*   `src/main/java/com/customer/util/DataframeUtils.java`: A utility class that provides helper functions for data transformation.
*   `pom.xml`: The Maven project file that defines dependencies and build settings.
*   `migrateJdbcToJdbc.md`: A documentation file with instructions on how to run the Spark job on a serverless Spark environment.

## How to Run

Please refer to the detailed instructions in the [migrateJdbcToJdbc.md](migrateJdbcToJdbc.md) file.
