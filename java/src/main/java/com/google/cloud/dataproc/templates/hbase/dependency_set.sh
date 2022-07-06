wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-client/2.4.12/hbase-client-2.4.12.jar
wget https://repo1.maven.org/maven2/org/apache/hbase/hbase-shaded-mapreduce/2.4.12/hbase-shaded-mapreduce-2.4.12.jar
gsutil copy hbase-client-2.4.12.jar $GCS_DEPENDENCY_LOCATION
gsutil copy hbase-shaded-mapreduce-2.4.12.jar $GCS_DEPENDENCY_LOCATION

rm hbase-client-2.4.12.jar
rm hbase-shaded-mapreduce-2.4.12.jar
