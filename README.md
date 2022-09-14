![Java Build Status](https://dataproctemplatesci.com/buildStatus/icon?job=dataproc-templates-build%2Fbuild-job-java&&subject=java-build)
![Java Integration Tests Status](https://dataproctemplatesci.com/buildStatus/icon?job=dataproc-templates-build%2Fintegration-tests-java&&subject=integration-tests-java)
![Python Build Status](https://dataproctemplatesci.com/buildStatus/icon?job=dataproc-templates-build%2Fbuild-job-python&&subject=python-build) 
![Python Integration Test Status](https://dataproctemplatesci.com/buildStatus/icon?job=dataproc-templates-build%2Fintegration-tests-python&&subject=integration-tests-python)

# Dataproc Templates
Dataproc templates are an effort to solve simple, but large, in-Cloud data tasks, including data import/export/backup/restore and bulk API operations. The technology under the hood which makes these operations possible is the serverless spark functionality based on [Google Cloud's Dataproc](https://cloud.google.com/dataproc/).

Google is providing this collection of pre-implemented Dataproc templates as a reference and to provide easy customization for developers wanting to extend their functionality.

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor)


## Dataproc Templates (Java - Spark)
Please refer to the [Dataproc Templates (Java - Spark) README](/java/README.md)  for more information
* [HiveToBigQuery](/java/src/main/java/com/google/cloud/dataproc/templates/hive/README.md)
* [HiveToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/hive/README.md)
* [PubSubToBigQuery](/java/src/main/java/com/google/cloud/dataproc/templates/pubsub/README.md)
* [GCSToBigQuery](/java/src/main/java/com/google/cloud/dataproc/templates/gcs/README.md)
* [GCSToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/gcs/README.md)
* [GCSToSpanner](/java/src/main/java/com/google/cloud/dataproc/templates/gcs/README.md) (blogpost [link](https://medium.com/google-cloud/fast-export-large-database-tables-using-gcp-serverless-dataproc-spark-bb32b1260268))
* [HBaseToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/hbase/README.md)
* [SpannerToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/databases/README.md) (blogpost [link](https://medium.com/google-cloud/cloud-spanner-export-query-results-using-dataproc-serverless-6f2f65b583a4))
* [S3ToBigQuery](/java/src/main/java/com/google/cloud/dataproc/templates/s3/README.md)
* [JDBCToBigQuery](/java/src/main/java/com/google/cloud/dataproc/templates/jdbc/README.md)
* [JDBCToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/jdbc/README.md) (blogpost [link](https://medium.com/google-cloud/fast-export-large-database-tables-using-gcp-serverless-dataproc-spark-bb32b1260268))
* [PubSubToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/pubsub/README.md#2-pubsub-to-gcs) (blogpost [link](https://medium.com/google-cloud/stream-data-from-pub-sub-to-cloud-storage-using-dataproc-serverless-7a1e4823926e))
* [GCSToJDBC](/java/src/main/java/com/google/cloud/dataproc/templates/gcs/README.md) (blogpost [link](https://medium.com/google-cloud/importing-data-from-gcs-to-databases-via-jdbc-using-dataproc-serverless-7ed75eab93ba))
* [SnowflakeToGCS](/java/src/main/java/com/google/cloud/dataproc/templates/snowflake/README.md)
* [KafkaToBQ](/java/src/main/java/com/google/cloud/dataproc/templates/kafka/README.md) (blogpost [link](https://medium.com/google-cloud/export-data-from-apache-kafka-to-bigquery-using-dataproc-serverless-4a666535117c))
* [DataplexGCStoBQ](/java/src/main//java/com/google/cloud/dataproc/templates/dataplex/README.md)
* [WordCount](/java/src/main/java/com/google/cloud/dataproc/templates/word/WordCount.java)
* [GeneralTemplate](/java/src/main/java/com/google/cloud/dataproc/templates/general/README.md)

## Dataproc Templates (Python - PySpark)
Please refer to the [Dataproc Templates (Python - PySpark) README](/python/README.md) for more information

* [BigQueryToGCS](/python/dataproc_templates/bigquery/README.md) (blogpost [link](https://medium.com/google-cloud/moving-data-from-bigquery-to-gcs-using-gcp-dataproc-serverless-and-pyspark-f6481b86bcd1))
* [GCSToBigQuery](/python/dataproc_templates/gcs/README.md) (blogpost [link](https://medium.com/@ppaglilla/getting-started-with-dataproc-serverless-pyspark-templates-e32278a6a06e))
* [GCSToBigTable](/python/dataproc_templates/gcs/README.md)
* [GCSToJDBC](/python/dataproc_templates/gcs/README.md)
* [GCSToMongo](/python/dataproc_templates/gcs/README.md) (blogpost [link](https://medium.com/google-cloud/importing-data-from-gcs-to-mongodb-using-dataproc-serverless-fed58904633a))
* [GCSToGCS](/python/dataproc_templates/gcs/README.md)
* [HiveToBigQuery](/python/dataproc_templates/hive/README.md) (blogpost [link](https://medium.com/google-cloud/processing-data-from-hive-to-bigquery-using-pyspark-and-dataproc-serverless-217c7cb9e4f8))
* [HiveToGCS](/python/dataproc_templates/hive/README.md) (blogpost [link](https://medium.com/@surjitsh/processing-large-data-tables-from-hive-to-gcs-using-pyspark-and-dataproc-serverless-35d3d16daaf))
* [HbaseToGCS](/python/dataproc_templates/hbase/README.md)
* [MongoToGCS](/python/dataproc_templates/mongo/README.md)
* [SnowflakeToGCS](/python/dataproc_templates/snowflake/README.md)
* [JDBCToJDBC](/python/dataproc_templates/jdbc/README.md)
* [JDBCToGCS](/python/dataproc_templates/jdbc/README.md)
* [RedshiftToGCS](/python/dataproc_templates/redshift/README.md)
* [TextToBiqQuery](/python/dataproc_templates/gcs/README.md)

## Getting Started

1) Clone this repository

        git clone https://github.com/GoogleCloudPlatform/dataproc-templates.git
2) Obtain authentication credentials

   Create local credentials by running the following command and following the
   oauth2 flow (read more about the command [here](https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login).

        gcloud auth application-default login

   Or manually set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable
   to point to a service account key JSON file path.

   Learn more at [Setting Up Authentication for Server to Server Production Applications](https://developers.google.com/identity/protocols/oauth2/service-account).

*Note:* Application Default Credentials is able to implicitly find the credentials as long as the application is running on Compute Engine, Kubernetes Engine, App Engine, or Cloud Functions.

3) Executing a Template 

    Follow the specific guide, depending on your use case:
   - [Dataproc Templates (Java - Spark)](java/README.md)
   - [Dataproc Templates (Python - PySpark)](python/README.md)

## Flow diagram

Below flow diagram shows execution flow for Dataproc Templates:

![Dataproc templates flow diagram](dp-templates.png)

## Contributing
See the contributing [instructions](/CONTRIBUTING.md) to get started contributing.

## License
All solutions within this repository are provided under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license. Please see the [LICENSE](/LICENSE) file for more detailed terms and conditions.

## Disclaimer
This repository and its contents are not an official Google Product.

## Contact
Share you feedback, ideas, thoughts [feedback-form](https://forms.gle/XXCJeWeCJJ9fNLQS6)

Questions, issues, and comments should be directed to dataproc-templates-support-external@googlegroups.com

[gcf]: https://cloud.google.com/functions/
[gcf-bg]: https://cloud.google.com/functions/docs/writing/background
[logs-export]: https://cloud.google.com/logging/docs/export/
