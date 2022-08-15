![Build Status](https://dataproctemplatesci.com/buildStatus/icon?job=dataproc-templates-build%2Fbuild-job-java&&subject=java-build)


## Dataproc Templates (Java - Spark)
Please refer to the [Dataproc Templates (Java - Spark) README](java/README.md)  for more information
* [HiveToBigQuery](/java/src/main/java/com/google/cloud/dataproc/templates/hive/README.md)
* [HiveToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/hive/README.md)
* [HbaseToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/hbase/README.md)
* [PubSubToBigQuery](/java/src/main/java/com/google/cloud/dataproc/templates/pubsub/README.md)
* [GCSToBigQuery](/java/src/main/java/com/google/cloud/dataproc/templates/gcs/README.md)
* [GCSToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/gcs/README.md)
* [GCSToSpanner](/java/src/main/java/com/google/cloud/dataproc/templates/gcs/README.md) (blogpost [link](https://medium.com/google-cloud/fast-export-large-database-tables-using-gcp-serverless-dataproc-spark-bb32b1260268))
* [SpannerToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/databases/README.md) (blogpost [link](https://medium.com/google-cloud/cloud-spanner-export-query-results-using-dataproc-serverless-6f2f65b583a4))
* [S3ToBigQuery](/java/src/main/java/com/google/cloud/dataproc/templates/s3/README.md)
* [JDBCToBigQuery](/java/src/main/java/com/google/cloud/dataproc/templates/jdbc/README.md)
* [JDBCToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/jdbc/README.md) (blogpost [link](https://medium.com/google-cloud/fast-export-large-database-tables-using-gcp-serverless-dataproc-spark-bb32b1260268))
* [PubSubToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/pubsub/README.md#2-pubsub-to-gcs) (blogpost [link](https://medium.com/google-cloud/stream-data-from-pub-sub-to-cloud-storage-using-dataproc-serverless-7a1e4823926e))
* [HBaseToGSC](/java/src/main/java/com/google/cloud/dataproc/templates/hbase/README.md)
* [GCSToJDBC](/java/src/main/java/com/google/cloud/dataproc/templates/gcs/README.md) (blogpost [link](https://medium.com/google-cloud/importing-data-from-gcs-to-databases-via-jdbc-using-dataproc-serverless-7ed75eab93ba))
* [SnowflakeToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/snowflake/README.md)
* [KafkaToBQ](/java/src/main/java/com/google/cloud/dataproc/templates/kafka/README.md) (blogpost [link](https://medium.com/google-cloud/export-data-from-apache-kafka-to-bigquery-using-dataproc-serverless-4a666535117c))
* [DataplexGCStoBQ](/java/src/main/java/com/google/cloud/dataproc/templates/dataplex/README.md)
* [WordCount](/java/src/main/java/com/google/cloud/dataproc/templates/word/WordCount.java)
* [GeneralTemplate](/java/src/main/java/com/google/cloud/dataproc/templates/general/README.md)


Dataproc Templates (Java - Spark) submit jobs to Dataproc Serverless using [batches submit spark ](https://cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit/spark) and to Dataproc Standard using [jobs submit spark](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/spark) .

## Requirements

* Java 8
* Maven 3

## Submit templates

1) Format Code [Optional]

   From either the root directory or v2/ directory, run:

    ```sh
    mvn spotless:apply
    ```

   This will format the code and add a license header. To verify that the code is
   formatted correctly, run:

    ```sh
    mvn spotless:check
    ```

   The directory to run the commands from is based on whether the changes are under v2/ or not.

1. Building the Project

    Build the entire project using the maven compile command.

    ```sh
    mvn clean install
    ```

2. Executing a Template File

    Once the template is staged on Google Cloud Storage, it can then be
    executed using the
    [gcloud CLI](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs)
    tool.

    To stage and execute the template, you can use the `start.sh` script. This takes
    * Environment variables on where and how to deploy the templates
    * Additional options for `gcloud dataproc jobs submit spark` or `gcloud beta dataproc batches submit spark`
    * Template options, such as the critical `--template` option which says which template to run and
      `--templateProperty` options for passing in properties at runtime (as an alternative to setting
      them in `src/main/resources/template.properties`).

    * Usage syntax:
        ```
        start.sh [submit-spark-options] -- --template templateName [--templateProperty key=value] [extra-template-options]
        ```

        For example:
        ```
        # Set required environment variables.
        export PROJECT=my-gcp-project
        export REGION=gcp-region
        export GCS_STAGING_LOCATION=gs://my-bucket/temp
        # Set optional environment variables.
        export SUBNET=projects/<gcp-project>/regions/<region>/subnetworks/test-subnet1
        # ID of Dataproc cluster running permanent history server to access historic logs.
        export HISTORY_SERVER_CLUSTER=projects/<gcp-project>/regions/<region>/clusters/<cluster>

        # The submit spark options must be seperated with a "--" from the template options
        bin/start.sh \
        --properties=<spark.something.key>=<value> \
        --version=... \
        -- \
        --template <TEMPLATE TYPE>
        --templateProperty <key>=<value>
        ```

    1. #### Executing Hive to GCS template
        Detailed instructions at [README.md](src/main/java/com/google/cloud/dataproc/templates/hive/README.md)
        ```
        bin/start.sh \
        --properties=spark.hadoop.hive.metastore.uris=thrift://hostname/ip:9083
        -- --template HIVETOGCS
        ```

    1. #### Executing Hive to BigQuery template
        Detailed instructions at [README.md](src/main/java/com/google/cloud/dataproc/templates/hive/README.md)
        ```
        bin/start.sh \
        --properties=spark.hadoop.hive.metastore.uris=thrift://hostname/ip:9083 \
        -- --template HIVETOBIGQUERY
        ```

    1. #### Executing Spanner to GCS template.
        Detailed instructions at [README.md](src/main/java/com/google/cloud/dataproc/templates/databases/README.md)
        ```
        bin/start.sh -- --template SPANNERTOGCS
        ```

    1. #### Executing PubSub to BigQuery template.
        ```
        bin/start.sh -- --template PUBSUBTOBQ
        ```

   1. #### Executing PubSub to GCS template.
       ```
       bin/start.sh -- --template PUBSUBTOGCS
       ```

    1. #### Executing GCS to BigQuery template.
        ```
        bin/start.sh -- --template GCSTOBIGQUERY
        ```

   1. #### Executing BigQuery to GCS template.
       ```
       bin/start.sh -- --template BIGQUERYTOGCS
       ```

    1. #### Executing General template.
       Detailed instructions at [README.md](src/main/java/com/google/cloud/dataproc/templates/general/README.md)
       ```
        bin/start.sh --files="gs://bucket/path/config.yaml" \
        -- --template GENERAL --config config.yaml
        ```
        With for example `config.yaml`:
        ```yaml
        input:
          shakespeare:
            format: bigquery
            options:
              table: "bigquery-public-data:samples.shakespeare"
        query:
          wordcount:
            sql: "SELECT word, sum(word_count) cnt FROM shakespeare GROUP by word ORDER BY cnt DESC"
        output:
          wordcount:
            format: csv
            options:
              header: true
              path: gs://bucket/output/wordcount/
            mode: Overwrite
        ```

## Executing templates in existing dataproc cluster

To run the templates against existing cluster you must specify the `JOB_TYPE=CLUSTER` and `CLUSTER=<full clusterId>` environment variables. Eg:

    export PROJECT=my-gcp-project
    export REGION=gcp-region
    export GCS_STAGING_LOCATION=gs://my-bucket/temp
    export JOB_TYPE=CLUSTER
    export CLUSTER=${DATA_PROC_CLUSTER_NAME}
    bin/start.sh \
    -- --template HIVETOBIGQUERY
