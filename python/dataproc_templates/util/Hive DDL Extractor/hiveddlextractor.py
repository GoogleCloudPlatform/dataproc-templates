#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A simple example demonstrating Spark SQL Hive integration.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/hive.py
"""

from os.path import abspath

from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.bigquery.dataset import Dataset
from pyspark.sql import SparkSession
from pyspark.sql import Row
import subprocess
import sys

from google.cloud import storage

def WriteToCloud ( ddls,bucket,path ):
    print(path)
    client = storage.Client()
    bucket = client.get_bucket( bucket )
    blob = bucket.blob( path )
    blob.upload_from_string( ddls ) 
  

def HiveDDLExtractor(self):
    """
    
    Dataproc template allowing the extraction of Hive DDLs for import to BigQuery

    """
    
    # warehouse_location points to the default location for managed databases and tables
    warehouse_location = abspath('spark-warehouse')

    """
    System arguments passed through the commandline when running script
    Structure:
        python hiveddlextractor.py host_ip project dbinput hdfs_path gcs_working_directory
    Sample:
        python hiveddlextractor.py 10.128.0.39 mbawa-sandbox employee /user/anuyogam/ hivetobqddl
    """

    host_ip = sys.argv[1]
    project = sys.argv[2]
    dbinput= sys.argv[3]
    hdfs_path = sys.argv[4]
    gcs_working_directory = sys.argv[5]
    gcs_target_path="SparkDDL"
    gcs_hdfs_staging_path="gs://hivetobqddl/RawZone/"
    bigquery_dataset="hivetobqtesting"
    bigquery_table="metadata"
    
    print("Connecting to Metastore: "+"thrift://"+host_ip+":9083")
    spark = SparkSession \
        .builder \
        .appName("hive-ddl-dumps") \
        .config("hive.metastore.uris", "thrift://"+host_ip+":9083") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar')\
        .enableHiveSupport() \
        .getOrCreate()

    print("Connecting to Hive Database: "+dbinput)
    # databaseExists() checks if there is a database in the hive cluster that matches the system argument
    dbCheck = spark.catalog._jcatalog.databaseExists(dbinput)
    # dbCheck serves as a boolean. if true then the script will continue
    ddls = ""
    if dbCheck:
        tables = spark.catalog.listTables(dbinput)
        metadata=[]
        columns=['database','table','partition_string','format','hdfs_path','gcs_raw_zone_path']
        for t in tables:
            """ 
            The for loop iterates through all the tables within the database 
            """
            # default.emp_part
            print("Extracting DDL for the Hive Table: "+dbinput+"."+t.name)
            show_create = spark.sql("SHOW CREATE TABLE {}.{}".format(dbinput, t.name))
            ddl_table = spark.sql(
                "DESCRIBE FORMATTED {}.{}".format(dbinput, t.name))
            show_extended=spark.sql("show table extended from `"+dbinput+"` like '"+t.name+"'")
            ddl_table.registerTempTable("metadata")
            show_extended.registerTempTable("show_extended")
            info=spark.sql("select information from show_extended")
            partition_check=show_create.filter(show_create["createtab_stmt"].contains("PARTITIONED")).head(1)
            partition_string=""
            if partition_check:
                partition=show_create.first()[0].split("PARTITIONED BY (")[1].split(")")[0]
                partitions=partition.split(",")
                for part in partitions:
                    partition_type=spark.sql("select data_type from metadata where col_name="+"'"+part.strip()+"'").first()[0]
                    partition_string=partition_string+" "+part+" "+partition_type+","
                partition_string=partition_string[:-1]
                partition_by="PARTITIONED BY ("+partition_string+")\n"
                first_part=show_create.first()[0].split(partitions[0])[0][:-5].split(t.name+"`")[1:]
                first_part="".join(first_part)+")\n"
            else:
                partition_by=")\n"
                first_part=show_create.first()[0].split(t.name)[1:]
                first_part=show_create.first()[0].split(')')[0].split(t.name+"`")[1:]
                first_part="".join(first_part)+")\n"
            db_name=spark.sql("select data_type from metadata where col_name='Database'").first()[0]
            table_name=spark.sql("select data_type from metadata where col_name='Table'").first()[0]
            hdfs_file_path=info.first()[0].split("Location: ")[1].split("\n")[0]
            format=show_create.first()[0].split("USING ")[1].split("\n")[0]
            
            #partition and format
            initial="CREATE TABLE IF NOT EXISTS "+t.name
            storage_format=format
            print(hdfs_file_path)
            if dbinput != 'default':
                location_path=gcs_hdfs_staging_path+db_name+hdfs_file_path.split(db_name)[1]
            else:
                location_path=gcs_hdfs_staging_path+db_name+hdfs_file_path.split('warehouse')[1]
            metadata.append([db_name,table_name,partition_string,storage_format,hdfs_file_path,location_path])
            ddl = initial+first_part+partition_by+";\n"
            ddls = ddls + ddl
        rdd=spark.sparkContext.parallelize(metadata)
        metadata_df=rdd.toDF(columns)
        metadata_df.write.format('bigquery') .option('table', bigquery_dataset+'.'+bigquery_table) .option("temporaryGcsBucket",gcs_working_directory) .mode('append') .save()
    
    # function that writes the ddls string to GCS
    WriteToCloud(ddls,gcs_working_directory,gcs_target_path)

    spark.stop()