## 1. HBase To Cloud Storage
### Required JAR files

Some HBase dependencies are required to be passed when submitting the job. These dependencies are automatically set by script when CATALOG environment variable is set for hbase table configuration. If not, 
these dependencies need to be passed by using the --jars flag, or, in the case of Dataproc Templates, using the JARS environment variable. 
- **[Apache HBase Spark Connector](https://mvnrepository.com/artifact/org.apache.hbase.connectors.spark/hbase-spark) dependencies (already mounted in Dataproc Serverless through JARS option):**
    - All other dependencies are automatically downloaded and set once CATALOG environment variable is used for hbase table configuration. Lib link - [hbase-client](https://repo1.maven.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar), [hbase-shaded-mapreduce](https://repo1.maven.org/maven2/org/apache/hbase/hbase-shaded-mapreduce/2.4.12/hbase-shaded-mapreduce-2.4.12.jar)
    
  ### Pass the [hbase-site.xml](/java/src/main/resources/hbase-site.xml) to the Job
    There are two ways to do it. One by automatic process and another by manually creating a custom container for dataproc cluster. Both of them are illustrated below-: 
    
1) **Automatic process of creating custom container**-: The process is automatically done in the start-up script, when environment variable HBASE_SITE_PATH is set.
2) **Configure the [hbase-site.xml](/java/src/main/resources/hbase-site.xml) manually and create container**
  - The hbase-site.xml needs to be available in some path of the container image used by Dataproc Serverless.
  - Reference [hbase-site.xml](/java/src/main/resources/hbase-site.xml) can be used by adding respective values for **hbase.rootdir** and **hbase.zookeeper.quorum**
  - A [custom container image](https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers#submit_a_spark_batch_workload_using_a_custom_container_image) is required in GCP Artifact Registry. Refer [Dockerfile](./Dockerfile) for reference.
  - Add the following layer to the Dockerfile, for copying your local hbase-site.xml to the container image (below command is added to [Dockerfile](./Dockerfile) for reference):
    ```
    COPY hbase-site.xml /etc/hbase/conf/
    ```
  - You can use and adapt the Dockerfile from the guide above, building and pushing it to GCP Artifact Registry with:
    ```
    IMAGE=us-docker.pkg.dev/<your_project>/<your_custom_image>:<your_version>
    docker build -t "${IMAGE}" .
    docker push "${IMAGE}"
    ```
    
**Note**

We need below jars need to pass through `export JARS` options.
1. hbase-spark-protocol-shaded-1.0.1-spark_3.2-scala_2.12.jar
2. hbase-spark-1.0.1-spark_3.2-scala_2.12.jar
3. protobuf-java-2.5.0.jar (Dataproc image is providing higher version of protobuf which is not compatible with HBase. Downgrading it with HBase compatible version. Check out HBase [pom.xml](https://github.com/apache/hbase-connectors/blob/master/pom.xml#L180))
4. htrace-core4-4.2.0-incubating.jar (For some versions of Hbase, htrace module is missing which throws `ClassNotFoundException: org.apache.htrace.core.HTraceConfiguration`)


###General Execution:

*It is important to set CATALOG Environment variable here to provide hbase connection and for script to download required dependencies*
```
export GCP_PROJECT=<gcp-project-id>
export REGION=<region>
export SUBNET=<subnet>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export IMAGE_NAME_VERSION=<name:version of image>
export HBASE_SITE_PATH=<path to hbase-site.xml>
export CATALOG=<catalog of hbase table>
export IMAGE=us-docker.pkg.dev/${GCP_PROJECT}/${IMAGE_NAME_VERSION} #use the image which was created to congigure hbase-site.xml
export JARS="gs://${GCS_STAGING_LOCATION}/hbase-spark-protocol-shaded-1.0.1-spark_3.2-scala_2.12.jar,gs://${GCS_STAGING_LOCATION}/hbase-spark-1.0.1-spark_3.2-scala_2.12.jar,gs://${GCS_STAGING_LOCATION}/protobuf-java-2.5.0.jar,gs://${GCS_STAGING_LOCATION}/htrace-core4-4.2.0-incubating.jar"

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
```
export GCP_PROJECT=myproject
export REGION=us-central1
export GCS_STAGING_LOCATION=gs://staging_bucket
export JOB_TYPE=SERVERLESS 
export SUBNET=projects/myproject/regions/us-central1/subnetworks/default
export IMAGE_NAME_VERSION=dataproc-hbase:1
export HBASE_SITE_PATH=src/main/resources/hbase-site.xml
export CATALOG='{"table":{"namespace":"default","name":"my_table"},"rowkey":"key","columns":{"key":{"cf":"rowkey","col":"key","type":"string"},"name":{"cf":"cf","col":"name","type":"string"}}}'
export IMAGE=us-docker.pkg.dev/${GCP_PROJECT}/${IMAGE_NAME_VERSION}  #set this to pass custom image during job submit
export JARS="gs://${GCS_STAGING_LOCATION}/hbase-spark-protocol-shaded-1.0.1-spark_3.2-scala_2.12.jar,gs://${GCS_STAGING_LOCATION}/hbase-spark-1.0.1-spark_3.2-scala_2.12.jar,gs://${GCS_STAGING_LOCATION}/protobuf-java-2.5.0.jar,gs://${GCS_STAGING_LOCATION}/htrace-core4-4.2.0-incubating.jar"

bin/start.sh \
--container-image=$IMAGE \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/'  \
-- --template HBASETOGCS \
--templateProperty hbasetogcs.output.fileformat=csv \
--templateProperty hbasetogcs.output.savemode=append \
--templateProperty hbasetogcs.output.path=gs://myproject/output  \
--templateProperty hbasetogcs.table.catalog=$CATALOG
```

##Example Catalog
```
{
   "table":{
      "namespace":"default",
      "name":"my_table"
   },
   "rowkey":"key",
   "columns":{
      "key":{
         "cf":"rowkey",
         "col":"key",
         "type":"string"
      },
      "name":{
         "cf":"cf",
         "col":"name",
         "type":"string"
      }
   }
}
```
**Note:**

Dataproc serverless image >= 1.2 is not supporting container registry. It is recommended to use Artifact registry. Please refer [documentation](https://cloud.google.com/artifact-registry/docs/transition/transition-from-gcr).