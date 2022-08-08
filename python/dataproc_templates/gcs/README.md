# GCS To BigQuery

Template for reading files from Google Cloud Storage and writing them to a BigQuery table. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `gcs.bigquery.input.location`: GCS location of the input files (format: `gs://bucket/...`)
* `gcs.bigquery.output.dataset`: BigQuery dataset for the output table
* `gcs.bigquery.output.table`: BigQuery output table name
* `gcs.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json)
* `gcs.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `gcs.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)

## Usage

```
$ python main.py --template GCSTOBIGQUERY --help

usage: main.py --template GCSTOBIGQUERY [-h] \
    --gcs.bigquery.input.location GCS.BIGQUERY.INPUT.LOCATION \
    --gcs.bigquery.output.dataset GCS.BIGQUERY.OUTPUT.DATASET \
    --gcs.bigquery.output.table GCS.BIGQUERY.OUTPUT.TABLE \
    --gcs.bigquery.input.format {avro,parquet,csv,json} \
    --gcs.bigquery.temp.bucket.name GCS.BIGQUERY.TEMP.BUCKET.NAME \
    [--gcs.bigquery.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --gcs.bigquery.input.location GCS.BIGQUERY.INPUT.LOCATION
                        GCS location of the input files
  --gcs.bigquery.output.dataset GCS.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --gcs.bigquery.output.table GCS.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --gcs.bigquery.input.format {avro,parquet,csv,json}
                        Input file format (one of: avro,parquet,csv,json)
  --gcs.bigquery.temp.bucket.name GCS.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --gcs.bigquery.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

./bin/start.sh \
-- --template=GCSTOBIGQUERY \
    --gcs.bigquery.input.format="<json|csv|parquet|avro>" \
    --gcs.bigquery.input.location="<gs://bucket/path>" \
    --gcs.bigquery.output.dataset="<dataset>" \
    --gcs.bigquery.output.table="<table>" \
    --gcs.bigquery.output.mode=<append|overwrite|ignore|errorifexists>\
    --gcs.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```

# GCS To BigTable

Template for reading files from Google Cloud Storage and writing them to a BigTable table. It supports reading JSON, CSV, Parquet and Avro formats.

It uses the Apache HBase Spark Connector to write to BigTable.

This [tutorial](https://cloud.google.com/dataproc/docs/tutorials/spark-hbase#dataproc_hbase_tutorial_view_code-python) shows how to run a Spark/PySpark job connecting to BigTable.  
However, it focuses in running the job using a Dataproc cluster, and not Dataproc Serverless.  
Here in this template, you will notice that there are different configuration steps for the PySpark job to successfully run using Dataproc Serverless, connecting to BigTable using the HBase interface.

You can also check out the [differences between HBase and Cloud Bigtable](https://cloud.google.com/bigtable/docs/hbase-differences).

## Requirements

1) Configure the [hbase-site.xml](./hbase-site.xml) ([reference](https://cloud.google.com/bigtable/docs/hbase-connecting#creating_the_hbase-sitexml_file)) with your BigTable instance reference
    - The hbase-site.xml needs to be available in some path of the container image used by Dataproc Serverless.  
    - For that, you need to build and host a [customer container image](https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers#submit_a_spark_batch_workload_using_a_custom_container_image) in GCP Container Registry.  
      - Add the following layer to the [Dockerfile](./Dockerfile), for it to copy your local hbase-site.xml to the container image (already done):
        ```
        COPY hbase-site.xml /etc/hbase/conf/
        ```
      - Build the [Dockerfile](./Dockerfile), building and pushing it to GCP Container Registry with:
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

2) Configure the desired HBase catalog json to passed as an argument (table reference and schema)
    - The hbase-catalog.json should be passed using the --gcs.bigtable.hbase.catalog.json
    ```
    (./bin/start.sh ...)
    -- --gcs.bigtable.hbase.catalog.json='''{
                        "table":{"namespace":"default","name":"<table_id>"},
                        "rowkey":"key",
                        "columns":{
                        "key":{"cf":"rowkey", "col":"key", "type":"string"},
                        "name":{"cf":"cf", "col":"name", "type":"string"}
                        }
                    }'''
    ```

3) [Create and manage](https://cloud.google.com/bigtable/docs/managing-tables) your Bigtable table schema, column families, etc, to match the provided HBase catalog.

## Required JAR files

Some HBase and BigTable dependencies are required to be passed when submitting the job.  
These dependencies need to be passed by using the --jars flag, or, in the case of Dataproc Templates, using the JARS environment variable.  
Some dependencies (jars) must be downloaded from [MVN Repository](https://mvnrepository.com/) and stored your GCS bucket (create one to store the dependencies).  

- **[Apache HBase Spark Connector](https://mvnrepository.com/artifact/org.apache.hbase.connectors.spark/hbase-spark) dependencies (already mounted in Dataproc Serverless, so you refer to them using file://):**
   - file:///usr/lib/spark/external/hbase-spark-protocol-shaded.jar
   - file:///usr/lib/spark/external/hbase-spark.jar
   
- **Bigtable dependency:**
  - gs://<your_bucket_to_store_dependencies>/bigtable-hbase-2.x-hadoop-2.3.0.jar
    - Download it using ``` wget https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-2.x-shaded/2.3.0/bigtable-hbase-2.x-shaded-2.3.0.jar```

- **HBase dependencies:**
  - gs://<your_bucket_to_store_dependencies>/hbase-client-2.4.12.jar
      - Download it using ``` wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar```
  - gs://<your_bucket_to_store_dependencies>/hbase-shaded-mapreduce-2.4.12.jar
      - Download it using ``` wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-shaded-mapreduce/2.4.12/hbase-shaded-mapreduce-2.4.12.jar```

## Arguments

* `gcs.bigquery.input.location`: GCS location of the input files (format: `gs://<bucket>/...`)
* `gcs.bigquery.input.format`: Input file format (one of: avro,parquet,csv,json)
* `gcs.bigtable.hbase.catalog.json`: HBase catalog inline json

## Usage

```
$ python main.py --template GCSTOBIGTABLE --help                
                        
usage: main.py [-h] --gcs.bigtable.input.location GCS.BIGTABLE.INPUT.LOCATION
                    --gcs.bigtable.input.format {avro,parquet,csv,json}
                    --gcs.bigtable.hbase.catalog.json GCS.BIGTABLE.HBASE.CATALOG.JSON

optional arguments:
  -h, --help            show this help message and exit
  --gcs.bigtable.input.location GCS.BIGTABLE.INPUT.LOCATION
                        GCS location of the input files
  --gcs.bigtable.input.format {avro,parquet,csv,json}
                        Input file format (one of: avro,parquet,csv,json)
  --gcs.bigtable.hbase.catalog.json GCS.BIGTABLE.HBASE.CATALOG.JSON
                        HBase catalog inline json
```

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://<your_bucket_to_store_dependencies>/bigtable-hbase-2.x-hadoop-2.3.0.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-client-2.4.12.jar, \
             gs://<your_bucket_to_store_dependencies>/hbase-shaded-mapreduce-2.4.12.jar, \
             file:///usr/lib/spark/external/hbase-spark-protocol-shaded.jar, \
             file:///usr/lib/spark/external/hbase-spark.jar"

./bin/start.sh \
--container-image="gcr.io/<your_project>/<your_custom_image>:<your_version>" \
--properties='spark.dataproc.driverEnv.SPARK_EXTRA_CLASSPATH=/etc/hbase/conf/' \ # image with hbase-site.xml in /etc/hbase/conf/
-- --template=GCSTOBIGTABLE \
   --gcs.bigtable.input.format="<json|csv|parquet|avro>" \
   --gcs.bigtable.input.location="<gs://bucket/path>" \
   --gcs.bigtable.hbase.catalog.json='''{
                        "table":{"namespace":"default","name":"my_table"},
                        "rowkey":"key",
                        "columns":{
                        "key":{"cf":"rowkey", "col":"key", "type":"string"},
                        "name":{"cf":"cf", "col":"name", "type":"string"}
                        }
                    }'''
```


# GCS To JDBC

Template for reading files from Google Cloud Storage and writing them to a JDBC table. It supports reading JSON, CSV, Parquet and Avro formats.

## Arguments

* `gcs.jdbc.input.format`: Input file format (one of: avro,parquet,csv,json)
* `gcs.jdbc.input.location`: GCS location of the input files (format: `gs://BUCKET/...`)
* `gcs.jdbc.output.table`: JDBC output table name
* `gcs.jdbc.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `gcs.jdbc.output.driver`: JDBC output driver name
* `gcs.jdbc.batch.size`: JDBC output batch size
* `gcs.jdbc.output.url`: JDBC output URL


## Usage

```
$ python main.py --template GCSTOJDBC --help

usage: main.py --template GCSTOJDBC [-h] \
    --gcs.jdbc.input.location GCS.JDBC.INPUT.LOCATION \
    --gcs.jdbc.input.format {avro,parquet,csv,json} \
    --gcs.jdbc.output.table GCS.JDBC.OUTPUT.TABLE \
    --gcs.jdbc.output.mode {overwrite,append,ignore,errorifexists} \
    --gcs.jdbc.output.driver GCS.JDBC.OUTPUT.DRIVER \
    --gcs.jdbc.batch.size GCS.JDBC.BATCH.SIZE \
    --gcs.jdbc.output.url GCS.JDBC.OUTPUT.URL


optional arguments:
  -h, --help            show this help message and exit
  --gcs.jdbc.input.location GCS.JDBC.INPUT.LOCATION
                        GCS location of the input files
  --gcs.jdbc.input.format {avro,parquet,csv,json}
                        Input file format (one of: avro,parquet,csv,json)                        
  --gcs.jdbc.output.table GCS.JDBC.OUTPUT.TABLE
                        JDBC output table name
  --gcs.jdbc.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)                        
  --gcs.jdbc.output.driver GCS.JDBC.OUTPUT.DRIVER
                        JDBC Output Driver Name
  --gcs.jdbc.batch.size GCS.JDBC.BATCH.SIZE
                        Batch size of the data means number of records wanted to insert in one round trip into JDBC Table                                               
  --gcs.jdbc.output.url GCS.JDBC.OUTPUT.URL
                        JDBC Driver URL to connect with consisting of username and passwprd as well
```

## Required JAR files

This template requires the JDBC jar file to be available in the Dataproc cluster.
User has to download the required jar file and host it inside a GCS Bucket, so that it could be referred during the execution of code.

wget Command to download JDBC MySQL jar file is as follows :-

```
wget http://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.30.tar.gz. -O /tmp/mysql-connector.tar.gz 

```

Once the jar file gets downloaded, please upload the file into a GCS Bucket.

## Example submission

```
export JARS=<gcs-bucket-location-containing-jar-file> 

./bin/start.sh \
-- --template=GCSTOBIGQUERY \
    --gcs.jdbc.input.format="<json|csv|parquet|avro>" \
    --gcs.jdbc.input.location="<gs://bucket/path>" \
    --gcs.bigquery.output.table="<table>" \
    --gcs.bigquery.output.mode=<append|overwrite|ignore|errorifexists> \
    --gcs.jdbc.output.driver="com.mysql.cj.jdbc.driver" \
    --gcs.jdbc.batch.size=1000 \
    --gcs.jdbc.output.url="jdbc:mysql://12.345.678.9:3306/test?user=root&password=root"
```

# GCS To MongoDB

Template for reading files from Google Cloud Storage and writing them to a MongoDB Collection. It supports reading JSON, CSV, Parquet and Avro formats.

## Arguments

* `gcs.mongo.input.format`: Input file format (one of: avro,parquet,csv,json)
* `gcs.mongo.input.location`: GCS location of the input files (format: `gs://BUCKET/...`)
* `gcs.mongo.output.uri`: MongoDB Output URI for connection
* `gcs.mongo.output.database`: MongoDB Output Database Name
* `gcs.mongo.output.collection`: MongoDB Output Collection Name
* `gcs.mongo.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `gcs.mongo.batch.size`: Output Batch Size (Defaults to 512)

## Usage

```
$ python main.py --template GCSTOMONGO --help

usage: main.py --template GCSTOMONGO [-h] \
    --gcs.mongo.input.location GCS.MONGO.INPUT.LOCATION \
    --gcs.mongo.input.format {avro,parquet,csv,json} \
    --gcs.mongo.output.uri GCS.MONGO.OUTPUT.URI \
    --gcs.mongo.output.database GCS.MONGO.OUTPUT.DATABASE \        
    --gcs.mongo.output.collection GCS.MONGO.OUTPUT.COLLECTION \
    --gcs.mongo.batch.size GCS.MONGO.BATCH.SIZE \
    --gcs.mongo.output.mode {overwrite,append,ignore,errorifexists}


optional arguments:
  -h, --help            show this help message and exit
  --gcs.mongo.input.location GCS.MONGO.INPUT.LOCATION
                        GCS location of the input files
  --gcs.mongo.input.format {avro,parquet,csv,json}
                        Input file format (one of: avro,parquet,csv,json)                        
  --gcs.mongo.output.uri GCS.MONGO.OUTPUT.URI
                        Driver URI to connect with MongoDB Instance consisting of username and passwprd as well
  --gcs.mongo.output.database GCS.MONGO.OUTPUT.DATABASE
                        MongoDB Database Name
  --gcs.mongo.output.collection GCS.MONGO.OUTPUT.COLLECTION
                        MongoDB output Collection name
  --gcs.mongo.batch.size GCS.MONGO.BATCH.SIZE
                        MongoDB output Batch size
  --gcs.mongo.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)                        
```

## Required JAR files

This template requires the MongoDB-Java Driver jar file to be available in the Dataproc cluster. Aprt from that, MongoDB-Spark connector jar file is also required to Export and Import Dataframe via Spark.
User has to download both the required jar files and host it inside a GCS Bucket, so that it could be referred during the execution of code.
Once the jar file gets downloaded, please upload the file into a GCS Bucket.

Wget Command to download these jar files is as follows :-

```
sudo wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/2.4.0/mongo-spark-connector_2.12-2.4.0.jar
sudo wget https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.9.1/mongo-java-driver-3.9.1.jar
```

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS=<gcs-bucket-location-containing-jar-file> 

./bin/start.sh \
-- --template=GCSTOMONGO \
    --gcs.mongo.input.format="<json|csv|parquet|avro>" \
    --gcs.mongo.input.location="<gs://bucket/path>" \
    --gcs.mongo.output.uri="mongodb://<username>:<password>@<Host_Name>:<Port_Number>" \
    --gcs.mongo.output.database="<Database_Name>" \
    --gcs.mongo.output.collection="<Collection_Name>" \
    --gcs.mongo.batch.size=512 \
    --gcs.mongo.output.mode="<append|overwrite|ignore|errorifexists>"
```


# Text To BigQuery

Template for reading TEXT files from Google Cloud Storage and writing them to a BigQuery table. It supports reading Text files with compression GZIP, BZIP2, LZ4, DEFLATE, NONE.

It uses the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) for writing to BigQuery.

## Arguments

* `text.bigquery.input.location`: GCS location of the input text files (format: `gs://BUCKET/...`)
* `text.bigquery.output.dataset`: BigQuery dataset for the output table
* `text.bigquery.output.table`: BigQuery output table name
* `text.bigquery.temp.bucket.name`: Temporary bucket for the Spark BigQuery connector
* `text.bigquery.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `text.bigquery.input.compression`: Input file compression format (one of: gzip,bzip4,lz4,deflate,none)
* `text.bigquery.input.delimiter`: Input file delimiter

## Usage

```
$ python main.py --template TEXTTOBIGQUERY --help

usage: main.py --template TEXTTOBIGQUERY [-h] \
    --text.bigquery.input.location TEXT.BIGQUERY.INPUT.LOCATION \
    --text.bigquery.output.dataset TEXT.BIGQUERY.OUTPUT.DATASET \
    --text.bigquery.output.table TEXT.BIGQUERY.OUTPUT.TABLE \
    --text.bigquery.temp.bucket.name TEXT.BIGQUERY.TEMP.BUCKET.NAME \
    --text.bigquery.input.compression {gzip,bzip4,lz4,deflate,none} \
    --text.bigquery.input.delimiter 
    [--text.bigquery.output.mode {overwrite,append,ignore,errorifexists}]

optional arguments:
  -h, --help            show this help message and exit
  --text.bigquery.input.location TEXT.BIGQUERY.INPUT.LOCATION
                        GCS location of the input files
  --text.bigquery.output.dataset TEXT.BIGQUERY.OUTPUT.DATASET
                        BigQuery dataset for the output table
  --text.bigquery.output.table TEXT.BIGQUERY.OUTPUT.TABLE
                        BigQuery output table name
  --text.bigquery.input.compression {gzip,bzip4,lz4,deflate,none}
                        Input file compression format (one of: gzip,bzip4,lz4,deflate,none)
  --text.bigquery.temp.bucket.name TEXT.BIGQUERY.TEMP.BUCKET.NAME
                        Spark BigQuery connector temporary bucket
  --text.bigquery.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
```

## Required JAR files

This template requires the [Spark BigQuery connector](https://cloud.google.com/dataproc-serverless/docs/guides/bigquery-connector-spark-example) to be available in the Dataproc cluster.

## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"

./bin/start.sh \
-- --template=TEXTTOBIGQUERY \
    --text.bigquery.input.compression="<gzip|bzip4|lz4|deflate|none>" \
    --text.bigquery.input.delimiter="<delimiter>" \
    --text.bigquery.input.location="<gs://bucket/path>" \
    --text.bigquery.output.dataset="<dataset>" \
    --text.bigquery.output.table="<table>" \
    --text.bigquery.output.mode=<append|overwrite|ignore|errorifexists> \
    --text.bigquery.temp.bucket.name="<temp-bq-bucket-name>"
```
# GCS To GCS - SQL Transformation

Template for reading files from Google Cloud Storage, applying data transformations using Spark SQL and then writing the tranformed data back to Google Cloud Storage. It supports reading and writing JSON, CSV, Parquet and Avro formats.

## Arguments

* `gcs.to.gcs.input.location`: GCS location of the input files (format: `gs://BUCKET/...`)
* `gcs.to.gcs.input.format`: Input file format (one of: avro,parquet,csv,json)
* `gcs.to.gcs.temp.view.name`: Temp view name for creating a spark sql view on 
  source data. 
  This name has to match with the table name that will be used in the SQLquery
* `gcs.to.gcs.sql.query`: SQL query for data transformation. This must use the
                          temp view name as the table to query from.
* `gcs.to.gcs.output.format`: Output file format (one of: avro,parquet,csv,json)
* `gcs.to.gcs.output.mode`: Output write mode (one of: append,overwrite,ignore,errorifexists)(Defaults to append)
* `gcs.to.gcs.output.partition.column`: Partition column name to partition the final output in destination bucket'
* `gcs.to.gcs.output.location`: destination GCS location
## Usage

```
$ python main.py --template GCSTOGCS --help

usage: main.py [-h] --gcs.to.gcs.input.location GCS.TO.GCS.INPUT.LOCATION
               --gcs.to.gcs.input.format {avro,parquet,csv,json}
               [--gcs.to.gcs.temp.view.name GCS.TO.GCS.TEMP.VIEW.NAME]
               [--gcs.to.gcs.sql.query GCS.TO.GCS.SQL.QUERY]
               [--gcs.to.gcs.output.partition.column GCS.TO.GCS.OUTPUT.PARTITION.COLUMN]
               [--gcs.to.gcs.output.format {avro,parquet,csv,json}]
               [--gcs.to.gcs.output.mode {overwrite,append,ignore,errorifexists}]
               --gcs.to.gcs.output.location GCS.TO.GCS.OUTPUT.LOCATION

optional arguments:
  -h, --help            show this help message and exit
  --gcs.to.gcs.input.location GCS.TO.GCS.INPUT.LOCATION
                        GCS location of the input files
  --gcs.to.gcs.input.format {avro,parquet,csv,json}
                        GCS input file format (one of: avro,parquet,csv,json)
  --gcs.to.gcs.temp.view.name GCS.TO.GCS.TEMP.VIEW.NAME
                        Temp view name for creating a spark sql view on 
                        source data. This name has to match with the 
                        table name that will be used in the SQL query
  --gcs.to.gcs.sql.query GCS.TO.GCS.SQL.QUERY
                        SQL query for data transformation. This must use the
                        temp view name as the table to query from.
  --gcs.to.gcs.output.partition.column GCS.TO.GCS.OUTPUT.PARTITION.COLUMN
                        Partition column name to partition the 
                        final output in destination bucket
  --gcs.to.gcs.output.format {avro,parquet,csv,json}
                        Output write format (one of:
                        avro,parquet,csv,json)(Defaults to parquet)
  --gcs.to.gcs.output.mode {overwrite,append,ignore,errorifexists}
                        Output write mode (one of:
                        append,overwrite,ignore,errorifexists) (Defaults to
                        append)
  --gcs.to.gcs.output.location GCS.TO.GCS.OUTPUT.LOCATION
                        destination GCS location                     
```

## Required JAR files

```

No specific jar files are needed for this template. For using AVRO file format, spark-avro.jar is neede. However, this is already accessible to dataproc serverless.

```


## Example submission

```
export GCP_PROJECT=<project_id>
export REGION=<region>
export GCS_STAGING_LOCATION=<gcs-staging-bucket-folder> 
export JARS=<gcs-bucket-location-containing-jar-file> 

./bin/start.sh \
-- --template=GCSTOGCS \ 
    --gcs.to.gcs.input.location="<gs://bucket/path>" \
    --gcs.to.gcs.input.format="<json|csv|parquet|avro>" \
    --gcs.to.gcs.temp.view.name="temp" \
    --gcs.to.gcs.sql.query="select *, 1 as col from temp" \
    --gcs.to.gcs.output.format="<json|csv|parquet|avro>" \
    --gcs.to.gcs.output.mode="<append|overwrite|ignore|errorifexists>" \
    --gcs.to.gcs.output.location="<gs://bucket/path>"
```
