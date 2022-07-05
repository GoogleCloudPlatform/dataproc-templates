## 1. Hbase To BigQuery
### Required JAR files

Some HBase dependencies are required to be passed when submitting the job.  
These dependencies need to be passed by using the --jars flag, or, in the case of Dataproc Templates, using the JARS environment variable.  
Some dependencies (jars) must be downloaded from [MVN Repository](https://mvnrepository.com/) and stored your GCS bucket (create one to store the dependencies).

- **[Apache HBase Spark Connector](https://mvnrepository.com/artifact/org.apache.hbase.connectors.spark/hbase-spark) dependencies (already mounted in Dataproc Serverless, so you refer to them using file://):**
    - file:///usr/lib/spark/external/hbase-spark-protocol-shaded.jar
    - file:///usr/lib/spark/external/hbase-spark.jar

- **HBase dependencies:**
    - gs://<your_bucket_to_store_dependencies>/hbase-client-2.4.12.jar
      ```
      wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar
      ```
    - gs://<your_bucket_to_store_dependencies>/hbase-shaded-mapreduce-2.4.12.jar
      ```
      wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-shaded-mapreduce/2.4.12/hbase-shaded-mapreduce-2.4.12.jar
      ```
    
  ### Configure the [hbase-site.xml](./hbase-site.xml)
    - The hbase-site.xml needs to be available in some path so that serverless container can gain access to it. For Java, it can be done by placing hbase-site.xml file in the resource folder and pass that root path to SPARK_EXTRA_CLASSPATH like below-:
    ```properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=src/main/resources/hbase-site.xml'```

General Execution:

```
export GCP_PROJECT=<gcp-project-id> \
export REGION=<region>  \
export SUBNET=<subnet>   \
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> \
export CATALOG=<catalog of hbase table>
export JARS=gs://<your_bucket_to_store_dependencies>/hbase-client-2.4.12.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-shaded-mapreduce-2.4.12.jar, \
             file:///usr/lib/spark/external/hbase-spark-protocol-shaded.jar, \
             file:///usr/lib/spark/external/hbase-spark.jar
 \
bin/start.sh \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=src/main/resources/hbase-site.xml'  \
-- --template HBASETOGCS \
--templateProperty hbasetogcs.fileformat=<avro|csv|parquet|json|orc>  \
--templateProperty hbasetogcs.savemode=<Append|Overwrite|ErrorIfExists|Ignore> \
--templateProperty hbasetogcs.output.path=<output-gcs-path>
--templateProperty hbasetogcs.table.catalog=$CATALOG
```
Example catalog -:

```{"table":{"namespace":"default", "name":"my_table"},"rowkey":"key","columns":{"key":{"cf":"rowkey", "col":"key", "type":"string"},"name":{"cf":"cf", "col":"name", "type":"string"}}}```