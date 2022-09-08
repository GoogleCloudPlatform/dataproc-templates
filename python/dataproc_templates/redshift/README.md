# Redshift To GCS

Template for reading data from Redshift table and writing into files in Google Cloud Storage. It supports reading partition tabels and supports writing in JSON, CSV, Parquet and Avro formats.

# Prerequisites

## Required JAR files

These templates requires the jar file to be available in the Dataproc cluster.
User has to download the required jar file and host it inside a GCS Bucket, so that it could be referred during the execution of code.

* spark-redshift.jar
```
wget https://repo1.maven.org/maven2/io/github/spark-redshift-community/spark-redshift_2.12/5.0.3/spark-redshift_2.12-5.0.3.jar
```
* spark-avro.jar
```
wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.3.0/spark-avro_2.12-3.3.0.jar
```
* RedshiftJDBC.jar
```
wget https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/2.1.0.9/redshift-jdbc42-2.1.0.9.jar
```
* minimal-json.jar
```
wget https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.5/minimal-json-0.9.5.jar
```

Once the jar file gets downloaded, please upload the file into a GCS Bucket and export the below variable

```
export JARS=<comma-seperated-gcs-bucket-location-containing-jar-file> 
```

## Redshift JDBC URL syntax

* Redshift
```
jdbc:redshift://[Redshift Endpoint]:[PORT]/<dbname>?user=<username>&password=<password>
```


## Other important properties

* Required Arguments

    * Temp S3 Location
    ```
    redshifttogcs.s3.tempdir="s3a://[Bucket-Name]/[tempdir-path]" 
    ```
    * Redshift IAM Role ARN
    ```
    redshifttogcs.iam.rolearn="arn:aws:iam::Account:role/RoleName"
    ```
    * S3 Access Key
    ```
    redshifttogcs.s3.accesskey="xxxxxxxx"
    ```
    * S3 Secret Key
    ```
    redshifttogcs.s3.secretkey="xxxxxxxx"
    ```

* You can specify the source table name. Example,

```
redshifttogcs.input.table="employees"
```

## Arguments

* `redshifttogcs.input.url`: Redshift JDBC input URL
* `redshifttogcs.s3.tempdir`: S3 temporary bucket location
* `redshifttogcs.input.table`: Redshift input table name
* `redshifttogcs.output.location`: GCS location for output files (format: `gs://BUCKET/...`)
* `redshifttogcs.output.format`: Output file format (one of: avro,parquet,csv,json)
* `redshifttogcs.iam.rolearn` : IAM Role with S3 Access
* `redshifttogcs.s3.accesskey` : AWS Access Key for S3 Access
* `redshifttogcs.s3.secretkey` : AWS Secret Key for S3 Access
* `redshifttogcs.output.mode` (Optional): Output write mode (one of: append,overwrite,ignore,errorifexists) (Defaults to append)
* `redshifttogcs.output.partitioncolumn` (Optional): Output partition column name

## Usage

```
$ python main.py --template REDSHIFTTOGCS --help

usage: main.py --template REDSHIFTTOGCS \
    --redshifttogcs.input.url REDSHIFTTOGCS.INPUT.URL \
    --redshifttogcs.s3.tempdir REDSHIFTTOGCS.S3.TEMPDIR \
    --redshifttogcs.input.table REDSHIFTTOGCS.INPUT.TABLE \
    --redshifttogcs.iam.rolearn REDSHIFTTOGCS.IAM.ROLEARN \
    --redshifttogcs.s3.accesskey REDSHIFTTOGCS.S3.ACCESSKEY \
    --redshifttogcs.s3.secretskey REDSHIFTTOGCS.S3.SECRETKEY \
    --redshifttogcs.output.location REDSHIFTTOGCS.OUTPUT.LOCATION \
    --redshifttogcs.output.format {avro,parquet,csv,json} \

optional arguments:
    -h, --help            show this help message and exit
    --redshifttogcs.output.mode {overwrite,append,ignore,errorifexists} \
    --redshifttogcs.output.partitioncolumn JDBCTOGCS.OUTPUT.PARTITIONCOLUMN \
```

## General execution: 

```
export GCP_PROJECT=<gcp-project-id> 
export REGION=<region>  
export GCS_STAGING_LOCATION=<gcs staging location> 
export SUBNET=<subnet>   
export JARS="<gcs_path_to_jdbc_jar_files>/spark-redshift_2.12-5.0.3.jar,<gcs_path_to_jdbc_jar_files>/redshift-jdbc42-2.1.0.9.jar,<gcs_path_to_jdbc_jar_files>/minimal-json-0.9.5.jar"

./bin/start.sh \
-- --template=REDSHIFTTOGCS \
--redshifttogcs.input.url="jdbc:redshift://[Redshift Endpoint]:[PORT]/<dbname>?user=<username>&password=<password>" \
--redshifttogcs.s3.tempdir="s3a://bucket-name/temp" \
--redshifttogcs.input.table="table-name" \
--redshifttogcs.iam.rolearn="arn:aws:iam::xxxxxxxx:role/Redshift-S3-Role" \
--redshifttogcs.s3.accesskey="xxxxxxxx" \
--redshifttogcs.s3.secretkey="xxxxxxxx" \
--redshifttogcs.output.location="gs://bucket" \
--redshifttogcs.output.mode=<optional-write-mode> \
--redshifttogcs.output.format=<output-write-format> \
--redshifttogcs.output.partitioncolumn=<optional-output-partition-column-name>
```

## Example execution: 

```
export GCP_PROJECT=my-gcp-proj
export REGION=us-central1 
export GCS_STAGING_LOCATION=gs://my-gcp-proj/staging
export SUBNET=projects/my-gcp-proj/regions/us-central1/subnetworks/default   
export JARS="gs://my-gcp-proj/jars/spark-redshift_2.12-5.0.3.jar,gs://my-gcp-proj/jars/redshift-jdbc42-2.1.0.9.jar,gs://my-gcp-proj/jars/minimal-json-0.9.5.jar"
```
```
./bin/start.sh \
-- --template=REDSHIFTTOGCS \
--redshifttogcs.input.url="jdbc:redshift://cluster-123.xxxxxxxx.region.redshift.amazonaws.com:5439/dev?user=user1&password=password1" \
--redshifttogcs.s3.tempdir="s3a://bucket-name/temp" \
--redshifttogcs.input.table="employee" \
--redshifttogcs.iam.rolearn="arn:aws:iam::xxxxxxxx:role/Redshift-S3-Role" \
--redshifttogcs.s3.accesskey="xxxxxxxx" \
--redshifttogcs.s3.secretkey="xxxxxxxx" \
--redshifttogcs.output.location="gs://output_bucket/output/" \
--redshifttogcs.output.mode="overwrite" \
--redshifttogcs.output.format="csv" \
--redshifttogcs.output.partitioncolumn="department_id"
```


