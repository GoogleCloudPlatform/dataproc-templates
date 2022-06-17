## Hbase To GCS

Template for reading files from Hbase and writing to Google Cloud Storage. It supports writing in JSON, CSV, Parquet and Avro formats.

## Requirements

1) Configure the [hbase-site.xml](./hbase-site.xml)
    - The hbase-site.xml needs to be available in some path of the container image used by Dataproc Serverless.  
    - Reference [hbase-site.xml](./hbase-site.xml) can be used by adding respective values for **hbase.rootdir** and **hbase.zookeeper.quorum**
    - A [customer container image](https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers#submit_a_spark_batch_workload_using_a_custom_container_image) is required in GCP Container Registry. Refer [Dockerfile](./Dockerfile) for reference. 
    - Add the following layer to the Dockerfile, for copying your local hbase-site.xml to the container image:
      ```
      COPY hbase-site.xml /etc/hbase/conf/
      ```
    - You can use and adapt the Dockerfile from the guide above, building and pushing it to GCP Container Registry with:
      ```
      IMAGE=gcr.io/<your_project>/<your_custom_image>:<your_version>
      docker build -t "${IMAGE}" .
      docker push "${IMAGE}"
      ```
    
    - An SPARK_EXTRA_CLASSPATH environment variable should also be set to the same path when submitting the job.
    ```
    (./bin/start.sh ...)
    --container-image="gcr.io/<your_project>/<your_custom_image>:<your_version>"  # image with hbase-site.xml in /etc/hbase/conf/
    --properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/'
    ```

## Required JAR files

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

## Arguments

* `hbase.gcs.output.location`: GCS location for output files (format: `gs://<bucket>/...`)
* `hbase.gcs.output.format`: Output file format (one of: avro,parquet,csv,json)
* `hbase.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `hbase.gcs.catalog.json`: Catalog schema file for Hbase table

## Usage

```
$ python main.py --template HBASETOGCS --help
                        
usage: main.py [-h] --hbase.gcs.output.location HBASE.GCS.OUTPUT.LOCATION
                    --hbase.gcs.output.format {avro,parquet,csv,json}
                    --hbase.gcs.catalog.json HBASE.GCS.CATALOG.JSON

optional arguments:
  -h, --help            show this help message and exit
  --hbase.gcs.output.location HBASE.GCS.OUTPUT.LOCATION
                        GCS location for onput files
  --hbase.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --hbase.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --hbase.gcs.catalog.json HBASE.GCS.CATALOG.JSON
                        Hbase catalog json
```

## Example submission

```
export GCP_PROJECT=<project_id>
export SUBNET=<subnet>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://<your_bucket_to_store_dependencies>/hbase-client-2.4.12.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-shaded-mapreduce-2.4.12.jar, \
             file:///usr/lib/spark/external/hbase-spark-protocol-shaded.jar, \
             file:///usr/lib/spark/external/hbase-spark.jar"

./bin/start.sh \
--container-image="gcr.io/<your_project>/<your_custom_image>:<your_version>" \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/' \ # image with hbase-site.xml in /etc/hbase/conf/
-- --template=HBASETOGCS \
   --hbase.gcs.output.location="<gs://bucket/path>" \
   --hbase.gcs.output.format="<json|csv|parquet|avro>" \
   --hbase.gcs.output.mode="<append|overwrite|ignore|errorifexists>" \
   --hbase.gcs.catalog.json='''{
                        "table":{"namespace":"default","name":"my_table"},
                        "rowkey":"key",
                        "columns":{
                        "key":{"cf":"rowkey", "col":"key", "type":"string"},
                        "name":{"cf":"cf", "col":"name", "type":"string"}
                        }
                    }'''
```