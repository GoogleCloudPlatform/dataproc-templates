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
# $example on:spark_hive$
from os.path import abspath

from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.bigquery.dataset import Dataset
from pyspark.sql import SparkSession
from pyspark.sql import Row
import subprocess
import sys


if __name__ == "__main__":
    # $example on:spark_hive$
    # warehouse_location points to the default location for managed databases and tables
    warehouse_location = abspath('spark-warehouse')
    #host_ip project dbinput hdfs_path gcs_working_directory
    #python hiveddldumper.py hive_ip mbawa-sandbox employee /user/anuyogam/ hivetobqddl
    host_ip = sys.argv[1]
    project = sys.argv[2]
    dbinput= sys.argv[3]
    hdfs_path = sys.argv[4]
    gcs_working_directory = sys.argv[5]
    #writes the result to SparkDDL directory
    gcs_target_path="gs://"+gcs_working_directory+"/SparkDDL"
    
    print("Connecting to Metastore: "+"thrift://"+host_ip+":9083")
    spark = SparkSession \
        .builder \
        .appName("hive-ddl-dumps") \
        .config("hive.metastore.uris", "thrift://"+host_ip+":9083") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    print("Connecting to Hive Database: "+dbinput)
    dbs = spark.catalog.listDatabases()
    # dbs="hive2bq"
    for db in dbs:
        if db.name == dbinput:
            f = open("hivedumps_{}.ddl".format(db.name), "w")
            tables = spark.catalog.listTables(db.name)
            for t in tables:
                # default.emp_part
                ACID_Check = spark.sql(
                    "DESCRIBE FORMATTED {}.{}".format(db.name, t.name))
                print("Processing Table: "+db.name+"."+t.name)
                if ACID_Check.filter(ACID_Check.data_type.contains('transactional=true')).head(1):
                    print("Acid table "+db.name+"."+t.name +
                        " not processed - ORC acid file has schema in them")
                elif ACID_Check.filter(ACID_Check.data_type.contains('SequenceFileInputFormat')).head(1):
                    DDL = spark.sql(
                        "SHOW CREATE TABLE {}.{} AS SERDE".format(db.name, t.name))
                    query = ("create schema if not exists")
                    f.write(DDL.first()[0].split(")")[0]+");\n")
                else:
                        print("Extracting DDL for the Hive Table: "+db.name+"."+t.name)
                        DDL = spark.sql(
                            "SHOW CREATE TABLE {}.{}".format(db.name, t.name))
                        query = ("create schema if not exists")
                        initial="CREATE TABLE IF NOT EXISTS "+t.name+" ("
                        f.write(initial+DDL.first()[0].split("(")[1].split(")")[0]+");\n")
            f.write("\n") 
            f.close()
    print("Copying Local Dump File to HDFS")
    subprocess.call(
        ['hadoop fs -mkdir -p hdfs://'+hdfs_path], shell=True)
    subprocess.call(
        ['hadoop fs -copyFromLocal -f hivedumps_'+dbinput+'.ddl hdfs://'+hdfs_path], shell=True)
    df = spark.read.text("hdfs://"+hdfs_path+"hivedumps_"+dbinput+".ddl")
    print("Writing the DDL extracted files to: "+gcs_target_path)
    df.write.format("text").save(
        gcs_target_path)
    spark.stop()
