## Hbase To Cloud Storage

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
      wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
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
Some dependencies (jars) must be downloaded from [MVN Repository](https://mvnrepository.com/) and stored your Cloud Storage bucket (create one to store the dependencies).

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

* `hbase.gcs.output.location`: Cloud Storage location for output files (format: `gs://<bucket>/...`)
* `hbase.gcs.output.format`: Output file format (one of: avro,parquet,csv,json)
* `hbase.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `hbase.gcs.catalog.json`: Catalog schema file for Hbase table
* `hbase.gcs.output.chartoescapequoteescaping`: Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise
* `hbase.gcs.output.compression`: Compression codec to use when saving to file. This can be one of the known case-insensitive short names (none, bzip2, gzip, lz4, snappy and deflate)
* `hbase.gcs.output.dateformat`: Sets the string that indicates a date format. This applies to date type
* `hbase.gcs.output.emptyvalue`: Sets the string representation of an empty value
* `hbase.gcs.output.encoding`: Specifies encoding (charset) of saved CSV files
* `hbase.gcs.output.escape`: Sets a single character used for escaping quotes inside an already quoted value
* `hbase.gcs.output.escapequotes`: A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
* `hbase.gcs.output.header`: Writes the names of columns as the first line. Defaults to True
* `hbase.gcs.output.ignoreleadingwhitespace`: A flag indicating whether or not leading whitespaces from values being read/written should be skipped
* `hbase.gcs.output.ignoretrailingwhitespace`: A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
* `hbase.gcs.output.linesep`: Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
* `hbase.gcs.output.nullvalue`: Sets the string representation of a null value
* `hbase.gcs.output.quote`: Sets a single character used for escaping quoted values where the separator can be part of the value. For writing, if an empty string is set, it uses u0000 (null character)
* `hbase.gcs.output.quoteall`: A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character
* `hbase.gcs.output.sep`: Sets a separator for each field and value. This separator can be one or more characters
* `hbase.gcs.output.timestampformat`: Sets the string that indicates a timestamp with timezone format
* `hbase.gcs.output.timestampntzformat`: Sets the string that indicates a timestamp without timezone format

## Usage

```
$ python main.py --template HBASETOGCS --help

usage: main.py [-h]
               --hbase.gcs.output.location HBASE.GCS.OUTPUT.LOCATION
               --hbase.gcs.output.format {avro,parquet,csv,json}
               [--hbase.gcs.output.mode {overwrite,append,ignore,errorifexists}]
               --hbase.gcs.catalog.json HBASE.GCS.CATALOG.JSON
               [--hbase.gcs.output.chartoescapequoteescaping HBASE.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING]
               [--hbase.gcs.output.compression HBASE.GCS.OUTPUT.COMPRESSION]
               [--hbase.gcs.output.dateformat HBASE.GCS.OUTPUT.DATEFORMAT]
               [--hbase.gcs.output.emptyvalue HBASE.GCS.OUTPUT.EMPTYVALUE]
               [--hbase.gcs.output.encoding HBASE.GCS.OUTPUT.ENCODING]
               [--hbase.gcs.output.escape HBASE.GCS.OUTPUT.ESCAPE]
               [--hbase.gcs.output.escapequotes HBASE.GCS.OUTPUT.ESCAPEQUOTES]
               [--hbase.gcs.output.header HBASE.GCS.OUTPUT.HEADER]
               [--hbase.gcs.output.ignoreleadingwhitespace HBASE.GCS.OUTPUT.IGNORELEADINGWHITESPACE]
               [--hbase.gcs.output.ignoretrailingwhitespace HBASE.GCS.OUTPUT.IGNORETRAILINGWHITESPACE]
               [--hbase.gcs.output.linesep HBASE.GCS.OUTPUT.LINESEP]
               [--hbase.gcs.output.nullvalue HBASE.GCS.OUTPUT.NULLVALUE]
               [--hbase.gcs.output.quote HBASE.GCS.OUTPUT.QUOTE]
               [--hbase.gcs.output.quoteall HBASE.GCS.OUTPUT.QUOTEALL]
               [--hbase.gcs.output.sep HBASE.GCS.OUTPUT.SEP]
               [--hbase.gcs.output.timestampformat HBASE.GCS.OUTPUT.TIMESTAMPFORMAT]
               [--hbase.gcs.output.timestampntzformat HBASE.GCS.OUTPUT.TIMESTAMPNTZFORMAT]

options:
  -h, --help            show this help message and exit
  --hbase.gcs.output.location HBASE.GCS.OUTPUT.LOCATION
                        Cloud Storage location for output files
  --hbase.gcs.output.format {avro,parquet,csv,json}
                        Output file format (one of: avro,parquet,csv,json)
  --hbase.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
  --hbase.gcs.catalog.json HBASE.GCS.CATALOG.JSON
                        Hbase catalog JSON
  --hbase.gcs.output.chartoescapequoteescaping HBASE.GCS.OUTPUT.CHARTOESCAPEQUOTEESCAPING
                        Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are
                        different, \0 otherwise
  --hbase.gcs.output.compression HBASE.GCS.OUTPUT.COMPRESSION
  --hbase.gcs.output.dateformat HBASE.GCS.OUTPUT.DATEFORMAT
                        Sets the string that indicates a date format. This applies to date type
  --hbase.gcs.output.emptyvalue HBASE.GCS.OUTPUT.EMPTYVALUE
                        Sets the string representation of an empty value
  --hbase.gcs.output.encoding HBASE.GCS.OUTPUT.ENCODING
                        Decodes the CSV files by the given encoding type
  --hbase.gcs.output.escape HBASE.GCS.OUTPUT.ESCAPE
                        Sets a single character used for escaping quotes inside an already quoted value
  --hbase.gcs.output.escapequotes HBASE.GCS.OUTPUT.ESCAPEQUOTES
                        A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character
  --hbase.gcs.output.header HBASE.GCS.OUTPUT.HEADER
                        Uses the first line of CSV file as names of columns. Defaults to True
  --hbase.gcs.output.ignoreleadingwhitespace HBASE.GCS.OUTPUT.IGNORELEADINGWHITESPACE
                        A flag indicating whether or not leading whitespaces from values being read/written should be skipped
  --hbase.gcs.output.ignoretrailingwhitespace HBASE.GCS.OUTPUT.IGNORETRAILINGWHITESPACE
                        A flag indicating whether or not trailing whitespaces from values being read/written should be skipped
  --hbase.gcs.output.linesep HBASE.GCS.OUTPUT.LINESEP
                        Defines the line separator that should be used for parsing. Defaults to \r, \r\n and \n for reading and \n for writing
  --hbase.gcs.output.nullvalue HBASE.GCS.OUTPUT.NULLVALUE
                        Sets the string representation of a null value
  --hbase.gcs.output.quote HBASE.GCS.OUTPUT.QUOTE
                        Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you
                        need to set not null but an empty string
  --hbase.gcs.output.quoteall HBASE.GCS.OUTPUT.QUOTEALL
  --hbase.gcs.output.sep HBASE.GCS.OUTPUT.SEP
                        Sets a separator for each field and value. This separator can be one or more characters
  --hbase.gcs.output.timestampformat HBASE.GCS.OUTPUT.TIMESTAMPFORMAT
                        Sets the string that indicates a timestamp with timezone format
  --hbase.gcs.output.timestampntzformat HBASE.GCS.OUTPUT.TIMESTAMPNTZFORMAT
                        Sets the string that indicates a timestamp without timezone format
```

## Example submission
**Manual Process**
```

export GCP_PROJECT=my-project
export SUBNET=projects/my-project/regions/us-central1/subnetworks/test-subnet
export REGION=us-central1
export GCS_STAGING_LOCATION="gs://my-bucket"

#For manual process JARS environment variable need to be set. Not required for automated process
export JARS="gs://my-input-bucket/hbase-client-2.4.12.jar, \
             gs://my-input-bucket/hbase-shaded-mapreduce-2.4.12.jar, \
             file:///usr/lib/spark/external/hbase-spark.jar"

./bin/start.sh \
--container-image="gcr.io/my-project/hbase-gcs-image:1.0.1" \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/' \ # image with hbase-site.xml in /etc/hbase/conf/
-- --template=HBASETOGCS \
   --hbase.gcs.output.location="gs://my-output-bucket/hbase-gcs-output" \
   --hbase.gcs.output.format="csv" \
   --hbase.gcs.output.mode="overwrite" \
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
export GCP_PROJECT=my-project
export SUBNET=projects/my-project/regions/us-central1/subnetworks/test-subnet
export REGION=us-central1
export GCS_STAGING_LOCATION="gs://my-bucket"
export CATALOG='{"table":{"namespace":"default","name":"my_table"},"rowkey":"key","columns":{"key":{"cf":"rowkey","col":"key","type":"string"},"name":{"cf":"cf","col":"name","type":"string"}}}'
export IMAGE_NAME_VERSION=hbase-gcs-image:1.0.1
export HBASE_SITE_PATH=/home/anishks/hbase-site.xml
export IMAGE=gcr.io/${GCP_PROJECT}/${IMAGE_NAME_VERSION}
export SKIP_IMAGE_BUILD=FALSE #It is a required envrironment variable. Set it to true if you want to use existing IMAGE

bin/start.sh \
--container-image=$IMAGE \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/'  \
-- --template=HBASETOGCS \
   --hbase.gcs.output.location=gs://my-project/output \
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
