## Hbase To GCS

Template for reading files from Hbase and writing to Google Cloud Storage. It supports writing in JSON, CSV, Parquet and Avro formats.

## Requirements

1) Configure the [hbase-site.xml](./hbase-site.xml)
    - The hbase-site.xml needs to be available in some path of the container image used by Dataproc Serverless.
    - This can be done in **Automated** way by passing path to hbase-site.xml in  HBASE_SITE_PATH environment variable. Example -: ```export HBASE_SITE_PATH=/<your_path>/hbase-site.xml```
    - This can also be done with manually by creating a custom container as mentioned below
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
    In some OS, --platform flag might have to be utilised while building the image. Please refer official doc [here](https://docs.docker.com/engine/reference/builder/#automatic-platform-args-in-the-global-scope). 
    Usage ```docker build -t "${IMAGE}" --platform <your-platform> . ```
    - An SPARK_EXTRA_CLASSPATH environment variable should also be set to the same path when submitting the job.
      ```
      (./bin/start.sh ...)
      --container-image="gcr.io/<your_project>/<your_custom_image>:<your_version>"  # image with hbase-site.xml in /etc/hbase/conf/
      --properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/'
      ```
2) Configure the desired HBase catalog json to passed as an argument (table reference and schema)
    - The hbase-catalog.json should be passed using the --hbase.gcs.catalog.json
    ```
    (./bin/start.sh ...)
    -- --hbase.gcs.catalog.json='''{
                        "table":{"namespace":"default","name":"my_table"},
                        "rowkey":"key",
                        "columns":{
                        "key":{"cf":"rowkey", "col":"key", "type":"string"},
                        "name":{"cf":"cf", "col":"name", "type":"string"}
                        }
                    }'''
    ```
3) Docker to build image. Please follow the official link -:  [Install Docker](https://docs.docker.com/engine/install/)  
4) If script is run locally, it is essential to connect local machine with GCP Container Registry and to push/pull images. Please refer the steps given in the official doc [here](https://cloud.google.com/container-registry/docs/pushing-and-pulling)

## Required JAR files

Some HBase dependencies are required to be passed when submitting the job. In order to avoid additional manual steps, startup script has **automated this process**. Just by setting **CATALOG** environment variable, script will automatically download and pass required dependency. Example: ```export CATALOG=<your-hbase-table-catalog>``` .
Or else manual steps has to be followed as discussed below-:
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
**Manual Process**
```

export GCP_PROJECT=<project_id>
export SUBNET=<subnet>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>

#For manual process JARS environment variable need to be set. Not required for automated process
export JARS="gs://<your_bucket_to_store_dependencies>/hbase-client-2.4.12.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-shaded-mapreduce-2.4.12.jar, \
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
**Automated process**
```
export GCP_PROJECT=<project_id>
export SUBNET=<subnet>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder>
export CATALOG='{"table":{"namespace":"default","name":"my_table"},"rowkey":"key","columns":{"key":{"cf":"rowkey","col":"key","type":"string"},"name":{"cf":"cf","col":"name","type":"string"}}}'
export IMAGE_NAME_VERSION=<image-name>:<version>
export HBASE_SITE_PATH=/home/anishks/hbase-site.xml
export IMAGE=gcr.io/${GCP_PROJECT}/${IMAGE_NAME_VERSION}
export SKIP_IMAGE_BUILD=FALSE #It is a required envrironment variable. Set it to true if you want to use existing IMAGE

bin/start.sh \
--container-image=$IMAGE \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/'  \
-- --template=HBASETOGCS \
   --hbase.gcs.output.location=gs://myproject/output \
   --hbase.gcs.output.format=csv \
   --hbase.gcs.output.mode=append \
   --hbase.gcs.catalog.json=$CATALOG
```

**Note-: For some versions of Hbase, htrace module is missing, hence might encounter-:**
```
Caused by: java.lang.ClassNotFoundException: org.apache.htrace.core.HTraceConfiguration
at java.base/java.net.URLClassLoader.findClass(URLClassLoader.java:476)
at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:589)
at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:522)
... 124 more
```
In automatic process, this module is downloaded and passed during runtime. However, in manual process, [htrace module](https://repo1.maven.org/maven2/org/apache/htrace/htrace-core4/4.2.0-incubating/htrace-core4-4.2.0-incubating.jar) jar can be downloaded and passed using JARS environment variable while submitting the job. The jar can be downloaded and put along with other dependency jar and can also be passed similarly. 
Example-:
```
export JARS=$JARS,gs://<your_bucket_to_store_dependencies>/htrace-core4-4.2.0-incubating.jar
```
