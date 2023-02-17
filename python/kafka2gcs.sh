gsutil rm -r gs://anshumanwins-test/kafka2bq_gcs/checkpoints/*

export GCP_PROJECT=yadavaja-sandbox
export REGION=us-west1 
export GCS_STAGING_LOCATION='gs://anshumanwins-test'
export SUBNET="projects/yadavaja-sandbox/regions/us-west1/subnetworks/test-subnet1"
export JARS="gs://dataproc-templates-jrs/latest/python/spark-sql-kafka-0-10_2.12-3.2.0.jar,gs://dataproc-templates-jrs/latest/python/kafka-clients-2.8.0.jar,gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar,gs://dataproc-templates-jrs/latest/python/commons-pool2-2.6.2.jar,gs://dataproc-templates-jrs/latest/python/spark-token-provider-kafka-0-10_2.12-3.2.0.jar"

./bin/start.sh \
-- --template=KAFKATOGCS \
    --kafka.gcs.checkpoint.location='gs://anshumanwins-test/kafka2gcs_temp/checkpoints/' \
    --kafka.gcs.output.location.gcs.path='gs://anshumanwins-test/kafka2gcs_temp/kafka_output' \
    --kafka.gcs.bootstrap.servers="10.0.0.2:9093" \
    --kafka.gcs.topic='test' \
    --kafka.starting.offset='earliest' \
    --kafka.gcs.output.format='json' \
    --kafka.gcs.output.mode='append' \
    --kafka.termination.timeout='60'

