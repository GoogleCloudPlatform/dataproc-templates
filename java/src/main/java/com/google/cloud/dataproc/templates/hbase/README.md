## 1. Hbase To BigQuery
### Required JAR files

Some HBase dependencies are required to be passed when submitting the job. These dependencies are automatically set by script when CATALOG environment variable is set for hbase table configuration. If not, 
these dependencies need to be passed by using the --jars flag, or, in the case of Dataproc Templates, using the JARS environment variable. 
- **[Apache HBase Spark Connector](https://mvnrepository.com/artifact/org.apache.hbase.connectors.spark/hbase-spark) dependencies (already mounted in Dataproc Serverless, so you refer to them using file://):**
    - file:///usr/lib/spark/external/hbase-spark-protocol-shaded.jar
    - file:///usr/lib/spark/external/hbase-spark.jar
    - All other dependencies are automatically downloaded and set once CATALOG environment variable is used for hbase table configuration. Lib link - [hbase-client](https://repo1.maven.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar), [hbase-shaded-mapreduce](https://repo1.maven.org/maven2/org/apache/hbase/hbase-shaded-mapreduce/2.4.12/hbase-shaded-mapreduce-2.4.12.jar)
    
  ### Configure the [hbase-site.xml](./hbase-site.xml)
    1) Configure the [hbase-site.xml](./hbase-site.xml)
    - The hbase-site.xml needs to be available in some path of the container image used by Dataproc Serverless.
    - Reference [hbase-site.xml](./hbase-site.xml) can be used by adding respective values for **hbase.rootdir** and **hbase.zookeeper.quorum**
    - A [customer container image](https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers#submit_a_spark_batch_workload_using_a_custom_container_image) is required in GCP Container Registry. Refer [Dockerfile](./Dockerfile) for reference.
    - Add the following layer to the Dockerfile, for copying your local hbase-site.xml to the container image (below command is added to [Dockerfile](./Dockerfile) for reference):
      ```
      COPY hbase-site.xml /etc/hbase/conf/
      ```
    - You can use and adapt the Dockerfile from the guide above, building and pushing it to GCP Container Registry with:
      ```
      IMAGE=gcr.io/<your_project>/<your_custom_image>:<your_version>
      docker build -t "${IMAGE}" .
      docker push "${IMAGE}"
      ```
      This process has been automated in the start-up script when environment variable HBASE_SITE_PATH is set. The container registry path can be found in IMAGE environment variable after the script has run.
###General Execution:

*It is important to set CATALOG Environment variable here to provide hbase connection and for script to download required dependencies*
```
export GCP_PROJECT=<gcp-project-id> \
export REGION=<region>  \
export SUBNET=<subnet>   \
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
export CATALOG=<catalog of hbase table>
export IMAGE=gcr.io/<your_project>/<your_custom_image>:<your_version> #use the image which was created to congigure hbase-site.xml

 \
bin/start.sh \
--container-image=$IMAGE \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/'  \
-- --template HBASETOGCS \
--templateProperty hbasetogcs.output.fileformat=<avro|csv|parquet|json|orc>  \
--templateProperty hbasetogcs.output.savemode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty hbasetogcs.output.path=<output-gcs-path>
--templateProperty hbasetogcs.table.catalog=$CATALOG
```
###Example Execution -:
```export GCP_PROJECT=myproject
export REGION=us-central1
export GCS_STAGING_LOCATION=gs://staging_bucket
export JOB_TYPE=SERVERLESS 
export SUBNET=projects/myproject/regions/us-central1/subnetworks/default
export CATALOG={“table":{"namespace":"default", "name":"my_table"},"rowkey":"key","columns":{"key":{"cf":"rowkey", "col":"key", "type":"string"},"name":{"cf":"cf", "col":"name", "type":"string”}}}
export IMAGE=gcr.io/myproject/dataproc-hbase:1  #use the image which was created to congigure hbase-site.xml

bin/start.sh \
--container-image=$IMAGE \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/'  \
-- --template HBASETOGCS \
--templateProperty hbasetogcs.output.fileformat=csv \
--templateProperty hbasetogcs.output.savemode=append \
--templateProperty hbasetogcs.output.path=gs://myproject/output  \
--templateProperty hbasetogcs.table.catalog=$CATALOG
```