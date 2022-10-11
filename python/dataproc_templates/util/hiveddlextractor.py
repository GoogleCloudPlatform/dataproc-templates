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


# $example on:spark_hive$
from os.path import abspath

from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.bigquery.dataset import Dataset
from pyspark.sql import SparkSession
from pyspark.sql import Row
import subprocess
import sys

# $example off:spark_hive$


if __name__ == "__main__":
    """
    
    Dataproc template allowing the extraction of Hive DDLs for import to BigQuery

    """

    # $example on:spark_hive$
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
    # dbs="hive2bq"

    # databaseExists() checks if there is a database in the hive cluster that matches the system argument
    dbCheck = spark.catalog._jcatalog.databaseExists(dbinput)
    # dbCheck serves as a boolean. if true then the script will continue
    if dbCheck:
        f = open("hivedumps_{}.ddl".format(db.name), "w")
        tables = spark.catalog.listTables(db.name)
        # the for loop iterates through all the tables within the database
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
    
    # the follwing will print confirmation that the script ran correctly(or not)
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
