![Build Status](http://35.227.235.112/buildStatus/icon?job=dataproc-templates-build%2Fbuild-job)
# Dataproc Templates
Dataproc templates are an effort to solve simple, but large, in-Cloud data tasks, including data import/export/backup/restore and bulk API operations. The technology under the hood which makes these operations possible is the serverless spark functionality based on [Google Cloud Dataproc](https://cloud.google.com/dataproc/)  service.

Google is providing this collection of pre-implemented Dataproc templates as a reference and to provide easy customization for developers wanting to extend their functionality.

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor)


## Templates
* [HiveToBigQuery](src/main/java/com/google/cloud/dataproc/templates/hive/HiveToBigQuery.java)
* [HiveToGCS](src/main/java/com/google/cloud/dataproc/templates/hive/HiveToGCS.java)
* [PubSubToBigQuery](src/main/java/com/google/cloud/dataproc/templates/pubsub/PubSubToBQ.java)
* [GCSToBigQuery](src/main/java/com/google/cloud/dataproc/templates/gcs/GCStoBigquery.java)
* [SpannerToGCS](src/main/java/com/google/cloud/dataproc/templates/databases/SpannerToGCS.java)
* [WordCount](src/main/java/com/google/cloud/dataproc/templates/word/WordCount.java)


## Getting Started

### Requirements

* Java 8
* Maven 3


1. Clone this repository:

        git clone https://github.com/GoogleCloudPlatform/dataproc-templates.git

1. Configure required properties at resources/template.properties

1. Obtain authentication credentials.

   Create local credentials by running the following command and following the
   oauth2 flow (read more about the command [here][auth_command]):

        gcloud auth application-default login

   Or manually set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable
   to point to a service account key JSON file path.

   Learn more at [Setting Up Authentication for Server to Server Production Applications][ADC].

   *Note:* Application Default Credentials is able to implicitly find the credentials as long as the application is running on Compute Engine, Kubernetes Engine, App Engine, or Cloud Functions.
1. Format Code [Optional]

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

1. Executing a Template File

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
        -- --template <TEMPLATE TYPE>
        -- --templateProperty <key>=<value>
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

    1. #### Executing GCS to BigQuery template.
        ```
        bin/start.sh -- --template GCSTOBIGQUERY
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

To run the templates against existing cluster you must specify the `JOB_TYPE=CLUSTER` and
`CLUSTER=<full clusterId>` environment variables. Eg:
    ```
    export PROJECT=my-gcp-project
    export REGION=gcp-region
    export GCS_STAGING_LOCATION=gs://my-bucket/temp
    export JOB_TYPE=CLUSTER
    export CLUSTER=${DATA_PROC_CLUSTER_NAME}
    bin/start.sh \
    -- --template HIVETOBIGQUERY
    ```


## Flow diagram

Below flow diagram shows execution flow for Dataproc templates:

![Dataproc templates flow diagram](dp-templates.png)


## Contributing
See the contributing [instructions](/CONTRIBUTING.md) to get started contributing.

## License
All solutions within this repository are provided under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license. Please see the [LICENSE](/LICENSE) file for more detailed terms and conditions.

## Disclaimer
This repository and its contents are not an official Google Product.

## Contact
Questions, issues, and comments should be directed to
[professional-services-oss@google.com](mailto:professional-services-oss@google.com).

[gcf]: https://cloud.google.com/functions/
[gcf-bg]: https://cloud.google.com/functions/docs/writing/background
[logs-export]: https://cloud.google.com/logging/docs/export/
